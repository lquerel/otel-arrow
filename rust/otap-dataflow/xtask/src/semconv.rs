// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Source inventory and drift checks for the internal semantic-convention registry.

mod diagram;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use proc_macro2::{TokenStream, TokenTree};
use quote::ToTokens;
use serde::{Deserialize, Serialize};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::visit::{self, Visit};
use syn::{
    Attribute, Expr, ExprArray, ExprLit, ExprMacro, ExprReference, ExprStruct, FieldValue,
    GenericArgument, Item, ItemConst, ItemEnum, ItemImpl, ItemStatic, ItemStruct, Lit, Meta,
    PathArguments, Token, Type,
};

const SEMCONV_DIR: &str = "semconv";
const METRIC_SET_CATALOG: &str = "semconv-codegen/metric-sets.yaml";

/// Checks the checked-in semantic conventions against production Rust declarations.
pub fn check() -> Result<()> {
    let inventory = Inventory::discover(Path::new("."))?;
    let metric_sets = MetricSetCatalog::load(Path::new(METRIC_SET_CATALOG))?;
    Registry::load(Path::new(SEMCONV_DIR))?.check(&inventory, &metric_sets)?;
    println!(
        "Semantic-convention registry matches {} metrics, {} attribute sets, and {} events.",
        inventory.metrics.len(),
        inventory.attribute_sets.len(),
        inventory.events.len()
    );
    Ok(())
}

/// Prints the source inventory used by the drift checker.
pub fn print_inventory() -> Result<()> {
    let inventory = Inventory::discover(Path::new("."))?;
    let mut report = serde_json::to_value(&inventory)?;
    report
        .as_object_mut()
        .ok_or_else(|| anyhow!("source inventory report must be a JSON object"))?
        .insert(
            "resolved_attributes".to_owned(),
            serde_json::to_value(expected_attributes(&inventory))?,
        );
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

/// Renders the observable entity and signal graph from the checked-in registry.
pub fn render_diagram(output: Option<&Path>) -> Result<()> {
    let registry = Registry::load(Path::new(SEMCONV_DIR))?;
    diagram::render(
        &registry,
        output.unwrap_or_else(|| Path::new(diagram::DEFAULT_OUTPUT)),
    )
}

#[derive(Debug, Default, Serialize)]
struct Inventory {
    metrics: BTreeMap<String, MetricDefinition>,
    attribute_sets: BTreeMap<String, AttributeSetDefinition>,
    events: BTreeMap<String, EventDefinition>,
    #[serde(skip)]
    attribute_enums: BTreeMap<String, BTreeMap<String, AttributeType>>,
    #[serde(skip)]
    constants: BTreeMap<String, BTreeSet<String>>,
    #[serde(skip)]
    manual_descriptors: BTreeMap<String, AttributeSetDefinition>,
    #[serde(skip)]
    manual_handlers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
struct MetricDefinition {
    canonical_name: String,
    metric_set: String,
    name: String,
    brief: String,
    unit: String,
    instrument: String,
    rust_instrument: String,
    value_type: String,
    temporality: Option<String>,
    package: String,
    rust_type: String,
    rust_types: BTreeSet<String>,
    registration_attribute_sets: BTreeSet<String>,
    measurement_attribute_sets: BTreeSet<String>,
    rust_field: String,
    source: String,
    availability: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct AttributeSetDefinition {
    descriptor: String,
    rust_type: String,
    package: String,
    source: String,
    fields: Vec<AttributeFieldDefinition>,
    composes: Vec<String>,
    availability: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct AttributeFieldDefinition {
    key: String,
    brief: String,
    r#type: AttributeType,
    rust_field: String,
    availability: Option<String>,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(untagged)]
enum AttributeType {
    Primitive(String),
    Enum { members: Vec<AttributeEnumMember> },
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
struct AttributeEnumMember {
    id: String,
    value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    brief: Option<String>,
    stability: String,
}

impl AttributeType {
    fn any() -> Self {
        Self::Primitive("any".to_owned())
    }

    fn merge(&mut self, incoming: &Self) {
        if self == incoming {
            return;
        }
        match (&mut *self, incoming) {
            (Self::Enum { .. }, Self::Primitive(incoming)) if incoming == "string" => {}
            (Self::Primitive(base), Self::Enum { .. }) if base == "string" => {
                self.clone_from(incoming);
            }
            (Self::Enum { members }, Self::Enum { members: additions }) => {
                let mut merged = members
                    .iter()
                    .cloned()
                    .map(|member| (member.value.clone(), member))
                    .collect::<BTreeMap<_, _>>();
                for member in additions {
                    merged
                        .entry(member.value.clone())
                        .and_modify(|current| {
                            if member < current {
                                current.clone_from(member);
                            }
                        })
                        .or_insert_with(|| member.clone());
                }
                *members = merged.into_values().collect();
            }
            _ => *self = Self::any(),
        }
    }

    fn merge_observation(&mut self, incoming: &Self) {
        if incoming != &Self::any() {
            self.merge(incoming);
        }
    }

    fn wire_type(&self) -> &str {
        match self {
            Self::Primitive(r#type) => r#type,
            Self::Enum { .. } => "string",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct EventDefinition {
    name: String,
    attributes: BTreeMap<String, String>,
    callsite_count: usize,
    attribute_occurrences: BTreeMap<String, usize>,
    scopes: BTreeSet<String>,
    severities: BTreeSet<String>,
    sources: BTreeSet<String>,
    availability: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct Target {
    package: String,
    source: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanPhase {
    Types,
    Definitions,
    Events,
}

impl Inventory {
    fn discover(workspace: &Path) -> Result<Self> {
        let targets = cargo_targets(workspace)?;
        let mut inventory = Self::default();

        for phase in [ScanPhase::Types, ScanPhase::Definitions, ScanPhase::Events] {
            let mut visited = HashSet::new();
            for target in &targets {
                let source = target.source.canonicalize().with_context(|| {
                    format!("failed to canonicalize {}", target.source.display())
                })?;
                let module_dir = source
                    .parent()
                    .ok_or_else(|| anyhow!("target has no parent: {}", source.display()))?
                    .to_path_buf();
                inventory.scan_file(
                    workspace,
                    &source,
                    &module_dir,
                    &target.package,
                    &target.package,
                    &[],
                    phase,
                    &mut visited,
                )?;
            }
        }

        inventory.finish_manual_attribute_sets();
        inventory.validate_composition()?;
        Ok(inventory)
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_file(
        &mut self,
        workspace: &Path,
        path: &Path,
        module_dir: &Path,
        package: &str,
        parent_event_scope: &str,
        parent_guards: &[String],
        phase: ScanPhase,
        visited: &mut HashSet<(String, PathBuf, String)>,
    ) -> Result<()> {
        let scope_key = match phase {
            ScanPhase::Types | ScanPhase::Definitions => String::new(),
            ScanPhase::Events => parent_event_scope.to_owned(),
        };
        let visit_key = (package.to_owned(), path.to_path_buf(), scope_key);
        if !visited.insert(visit_key) {
            return Ok(());
        }

        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read Rust source {}", path.display()))?;
        let syntax = syn::parse_file(&source)
            .with_context(|| format!("failed to parse Rust source {}", path.display()))?;
        self.scan_items(
            workspace,
            &syntax.items,
            path,
            module_dir,
            package,
            parent_event_scope,
            parent_guards,
            phase,
            visited,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_items(
        &mut self,
        workspace: &Path,
        items: &[Item],
        source_path: &Path,
        module_dir: &Path,
        package: &str,
        parent_event_scope: &str,
        parent_guards: &[String],
        phase: ScanPhase,
        visited: &mut HashSet<(String, PathBuf, String)>,
    ) -> Result<()> {
        let event_scope = if phase == ScanPhase::Events {
            component_scope_target(items)?.unwrap_or_else(|| parent_event_scope.to_owned())
        } else {
            parent_event_scope.to_owned()
        };

        for item in items {
            let attrs = item_attributes(item);
            if is_test_only(attrs)? {
                continue;
            }
            let guards = combined_guards(parent_guards, attrs)?;

            if let Item::Mod(module) = item {
                if matches!(
                    module.ident.to_string().as_str(),
                    "test" | "tests" | "testing"
                ) {
                    continue;
                }
                if let Some((_, nested)) = &module.content {
                    let child_dir = path_attribute(&module.attrs)?.map_or_else(
                        || module_dir.join(module.ident.to_string()),
                        |path| source_path.parent().unwrap_or(module_dir).join(path),
                    );
                    self.scan_items(
                        workspace,
                        nested,
                        source_path,
                        &child_dir,
                        package,
                        &event_scope,
                        &guards,
                        phase,
                        visited,
                    )?;
                } else {
                    let (child_path, child_dir) = resolve_module(source_path, module_dir, module)?;
                    self.scan_file(
                        workspace,
                        &child_path,
                        &child_dir,
                        package,
                        &event_scope,
                        &guards,
                        phase,
                        visited,
                    )?;
                }
                continue;
            }

            let source = display_source(workspace, source_path);
            match phase {
                ScanPhase::Types => self.scan_type_item(item, package)?,
                ScanPhase::Definitions => {
                    self.scan_definition_item(item, package, &source, &guards)?;
                }
                ScanPhase::Events => {
                    let mut visitor = EventVisitor {
                        inventory: self,
                        package,
                        event_scope: &event_scope,
                        source: &source,
                        availability: guard_string(&guards),
                        errors: Vec::new(),
                    };
                    visitor.visit_item(item);
                    if !visitor.errors.is_empty() {
                        bail!(visitor.errors.join("\n"));
                    }
                }
            }
        }
        Ok(())
    }

    fn scan_type_item(&mut self, item: &Item, package: &str) -> Result<()> {
        match item {
            Item::Enum(item_enum) => self.scan_attribute_enum(item_enum, package),
            Item::Impl(item_impl) => self.scan_attribute_enum_impl(item_impl, package),
            _ => Ok(()),
        }
    }

    fn scan_attribute_enum(&mut self, item: &ItemEnum, package: &str) -> Result<()> {
        if !derives_trait(&item.attrs, "AttributeEnum")? {
            return Ok(());
        }

        let mut members = Vec::new();
        for variant in &item.variants {
            if is_test_only(&variant.attrs)? {
                continue;
            }
            if !matches!(variant.fields, syn::Fields::Unit) {
                bail!(
                    "AttributeEnum {} contains non-unit variant {}",
                    item.ident,
                    variant.ident
                );
            }
            let default_value = to_snake_case(&variant.ident.to_string());
            let value = variant
                .attrs
                .iter()
                .find(|attr| attr.path().is_ident("attribute_value"))
                .map(parse_attribute_value)
                .transpose()?
                .unwrap_or(default_value);
            let brief = doc_brief(&variant.attrs);
            members.push(AttributeEnumMember {
                id: enum_member_id(&value),
                value,
                brief: (!brief.is_empty()).then_some(brief),
                stability: "development".to_owned(),
            });
        }
        self.insert_attribute_enum(package, item.ident.to_string(), members)
    }

    fn scan_attribute_enum_impl(&mut self, item: &ItemImpl, package: &str) -> Result<()> {
        let Some((_, trait_path, _)) = &item.trait_ else {
            return Ok(());
        };
        if trait_path
            .segments
            .last()
            .is_none_or(|segment| segment.ident != "AttributeEnum")
        {
            return Ok(());
        }
        let rust_type = type_name(&item.self_ty)?;
        let Some(variants) = item.items.iter().find_map(|item| {
            let syn::ImplItem::Const(item_const) = item else {
                return None;
            };
            (item_const.ident == "VARIANTS").then_some(&item_const.expr)
        }) else {
            return Ok(());
        };
        let values = string_array(variants).ok_or_else(|| {
            anyhow!("AttributeEnum implementation for {rust_type} has a non-literal VARIANTS value")
        })?;
        let members = values
            .into_iter()
            .map(|value| AttributeEnumMember {
                id: enum_member_id(&value),
                value,
                brief: None,
                stability: "development".to_owned(),
            })
            .collect();
        self.insert_attribute_enum(package, rust_type, members)
    }

    fn insert_attribute_enum(
        &mut self,
        package: &str,
        rust_type: String,
        members: Vec<AttributeEnumMember>,
    ) -> Result<()> {
        let definition = AttributeType::Enum { members };
        let definitions = self.attribute_enums.entry(rust_type.clone()).or_default();
        if let Some(previous) = definitions.insert(package.to_owned(), definition.clone())
            && previous != definition
        {
            bail!("conflicting AttributeEnum definitions for {package}::{rust_type}");
        }
        Ok(())
    }

    fn resolve_attribute_type(&self, package: &str, ty: &Type) -> AttributeType {
        if let Some(primitive) = primitive_attribute_type(ty) {
            return AttributeType::Primitive(primitive.to_owned());
        }
        let Ok(rust_type) = type_name(ty) else {
            return AttributeType::any();
        };
        let Some(definitions) = self.attribute_enums.get(&rust_type) else {
            return AttributeType::any();
        };
        if let Some(definition) = definitions.get(package) {
            return definition.clone();
        }
        let unique = definitions.values().collect::<BTreeSet<_>>();
        if let [definition] = unique.into_iter().collect::<Vec<_>>().as_slice() {
            return (*definition).clone();
        }
        AttributeType::any()
    }

    fn scan_definition_item(
        &mut self,
        item: &Item,
        package: &str,
        source: &str,
        guards: &[String],
    ) -> Result<()> {
        match item {
            Item::Const(item_const) => self.scan_const(item_const)?,
            Item::Struct(item_struct) => {
                self.scan_metric_set(item_struct, package, source, guards)?;
                self.scan_attribute_set(item_struct, package, source, guards)?;
            }
            Item::Static(item_static) => {
                self.scan_attribute_descriptor(item_static, package, source, guards)?;
            }
            Item::Impl(item_impl) => self.scan_attribute_handler(item_impl),
            _ => {}
        }
        Ok(())
    }

    fn scan_const(&mut self, item: &ItemConst) -> Result<()> {
        if let Some(value) = resolve_string_expr(&item.expr, &self.constants) {
            self.constants
                .entry(item.ident.to_string())
                .or_default()
                .insert(value);
        }
        Ok(())
    }

    fn scan_metric_set(
        &mut self,
        item: &ItemStruct,
        package: &str,
        source: &str,
        guards: &[String],
    ) -> Result<()> {
        let Some(attr) = find_attr(&item.attrs, "metric_set") else {
            return Ok(());
        };
        let metric_set = required_name_value(attr, "name")?;
        let registration_attributes = optional_type_value(attr, "registration_attributes")?;
        let measurement_attributes = optional_type_value(attr, "measurement_attributes")?;
        let fields = match &item.fields {
            syn::Fields::Named(fields) => &fields.named,
            _ => bail!("metric_set {} must use named fields", item.ident),
        };

        for field in fields {
            let Some(metric_attr) = find_attr(&field.attrs, "metric") else {
                continue;
            };
            if is_test_only(&field.attrs)? {
                continue;
            }
            let field_ident = field
                .ident
                .as_ref()
                .ok_or_else(|| anyhow!("metric field must be named"))?
                .to_string();
            let explicit_name = optional_name_value(metric_attr, "name")?;
            let name = explicit_name.unwrap_or_else(|| field_ident.replace('_', "."));
            let unit = required_name_value(metric_attr, "unit")?;
            let shape = metric_shape(&field.ty)?;
            let canonical_name = format!("{metric_set}.{name}");
            let field_guards = combined_guards(guards, &field.attrs)?;
            let definition = MetricDefinition {
                canonical_name: canonical_name.clone(),
                metric_set: metric_set.clone(),
                name,
                brief: doc_brief(&field.attrs),
                unit,
                instrument: shape.instrument,
                rust_instrument: shape.rust_instrument,
                value_type: shape.value_type,
                temporality: shape.temporality,
                package: package.to_owned(),
                rust_type: item.ident.to_string(),
                rust_types: [item.ident.to_string()].into_iter().collect(),
                registration_attribute_sets: registration_attributes.clone().into_iter().collect(),
                measurement_attribute_sets: measurement_attributes.clone().into_iter().collect(),
                rust_field: field_ident,
                source: source.to_owned(),
                availability: guard_string(&field_guards),
            };
            if let Some(previous) = self.metrics.get_mut(&canonical_name) {
                if previous.metric_set != definition.metric_set
                    || previous.name != definition.name
                    || previous.unit != definition.unit
                    || previous.instrument != definition.instrument
                    || previous.value_type != definition.value_type
                    || previous.temporality != definition.temporality
                {
                    bail!(
                        "incompatible declarations for canonical metric {canonical_name}: \
                         {}::{} and {}::{}",
                        previous.source,
                        previous.rust_field,
                        source,
                        definition.rust_field
                    );
                }
                previous.rust_types.insert(definition.rust_type.clone());
                previous
                    .registration_attribute_sets
                    .extend(definition.registration_attribute_sets);
                previous
                    .measurement_attribute_sets
                    .extend(definition.measurement_attribute_sets);
            } else {
                self.metrics.insert(canonical_name, definition);
            }
        }
        Ok(())
    }

    fn scan_attribute_set(
        &mut self,
        item: &ItemStruct,
        package: &str,
        source: &str,
        guards: &[String],
    ) -> Result<()> {
        let Some(attr) = find_attr(&item.attrs, "attribute_set") else {
            return Ok(());
        };
        let explicit_descriptor = optional_name_value(attr, "name")?;
        let inventory_key = explicit_descriptor.as_ref().map_or_else(
            || format!("{package}:{source}:{}", item.ident),
            |_| item.ident.to_string(),
        );
        let descriptor = explicit_descriptor
            .unwrap_or_else(|| format!("{}.item", item.ident.to_string().to_lowercase()));
        let fields = match &item.fields {
            syn::Fields::Named(fields) => &fields.named,
            syn::Fields::Unit => {
                self.attribute_sets.insert(
                    inventory_key,
                    AttributeSetDefinition {
                        descriptor,
                        rust_type: item.ident.to_string(),
                        package: package.to_owned(),
                        source: source.to_owned(),
                        availability: guard_string(guards),
                        ..AttributeSetDefinition::default()
                    },
                );
                return Ok(());
            }
            _ => bail!("attribute_set {} must use named fields", item.ident),
        };

        let mut definition = AttributeSetDefinition {
            descriptor,
            rust_type: item.ident.to_string(),
            package: package.to_owned(),
            source: source.to_owned(),
            availability: guard_string(guards),
            ..AttributeSetDefinition::default()
        };
        for field in fields {
            if is_test_only(&field.attrs)? {
                continue;
            }
            let ident = field
                .ident
                .as_ref()
                .ok_or_else(|| anyhow!("attribute field must be named"))?
                .to_string();
            if find_attr(&field.attrs, "compose").is_some() {
                definition.composes.push(type_name(&field.ty)?);
                continue;
            }
            let key = find_attr(&field.attrs, "attribute_key")
                .map(parse_attribute_key)
                .transpose()?
                .unwrap_or_else(|| ident.replace('_', "."));
            let field_guards = combined_guards(guards, &field.attrs)?;
            definition.fields.push(AttributeFieldDefinition {
                key,
                brief: doc_brief(&field.attrs),
                r#type: self.resolve_attribute_type(package, &field.ty),
                rust_field: ident,
                availability: guard_string(&field_guards),
            });
        }
        if let Some(previous) = self.attribute_sets.insert(inventory_key, definition) {
            bail!(
                "duplicate attribute-set Rust type {} in {} and {}",
                item.ident,
                previous.source,
                source
            );
        }
        Ok(())
    }

    fn scan_attribute_descriptor(
        &mut self,
        item: &ItemStatic,
        package: &str,
        source: &str,
        guards: &[String],
    ) -> Result<()> {
        if type_name(&item.ty).ok().as_deref() != Some("AttributesDescriptor") {
            return Ok(());
        }
        let Expr::Struct(expr) = item.expr.as_ref() else {
            return Ok(());
        };
        let mut descriptor = parse_manual_descriptor(expr)?;
        descriptor.package = package.to_owned();
        descriptor.source = source.to_owned();
        descriptor.availability = guard_string(guards);
        self.manual_descriptors
            .insert(item.ident.to_string(), descriptor);
        Ok(())
    }

    fn scan_attribute_handler(&mut self, item: &ItemImpl) {
        let Some((_, trait_path, _)) = &item.trait_ else {
            return;
        };
        if trait_path
            .segments
            .last()
            .is_none_or(|segment| segment.ident != "AttributeSetHandler")
        {
            return;
        }
        let Ok(rust_type) = type_name(&item.self_ty) else {
            return;
        };
        for impl_item in &item.items {
            let syn::ImplItem::Fn(function) = impl_item else {
                continue;
            };
            if function.sig.ident != "descriptor" {
                continue;
            }
            let Some(syn::Stmt::Expr(expr, _)) = function.block.stmts.last() else {
                continue;
            };
            let Expr::Reference(ExprReference { expr, .. }) = expr else {
                continue;
            };
            let Expr::Path(path) = expr.as_ref() else {
                continue;
            };
            if let Some(descriptor) = path.path.segments.last() {
                self.manual_handlers
                    .insert(rust_type.clone(), descriptor.ident.to_string());
            }
        }
    }

    fn finish_manual_attribute_sets(&mut self) {
        for (rust_type, descriptor_name) in &self.manual_handlers {
            let Some(mut definition) = self.manual_descriptors.get(descriptor_name).cloned() else {
                continue;
            };
            definition.rust_type.clone_from(rust_type);
            self.attribute_sets
                .entry(rust_type.clone())
                .or_insert(definition);
        }
    }

    fn validate_composition(&self) -> Result<()> {
        for definition in self.attribute_sets.values() {
            for composed in &definition.composes {
                if !self.attribute_sets.contains_key(composed) {
                    bail!(
                        "attribute set {} composes unknown production attribute set {}",
                        definition.rust_type,
                        composed
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct MetricShape {
    instrument: String,
    rust_instrument: String,
    value_type: String,
    temporality: Option<String>,
}

fn metric_shape(ty: &Type) -> Result<MetricShape> {
    let Type::Path(path) = ty else {
        bail!("unsupported metric field type: {}", ty.to_token_stream());
    };
    let segment = path
        .path
        .segments
        .last()
        .ok_or_else(|| anyhow!("empty metric type path"))?;
    let rust_instrument = segment.ident.to_string();
    if matches!(
        rust_instrument.as_str(),
        "Mmsc" | "HistogramNormal" | "HistogramDetailed"
    ) {
        return Ok(MetricShape {
            instrument: "histogram".to_owned(),
            rust_instrument,
            value_type: "f64".to_owned(),
            temporality: Some("delta".to_owned()),
        });
    }

    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        bail!("metric type {} is missing its value type", rust_instrument);
    };
    let value_type = match arguments.args.first() {
        Some(GenericArgument::Type(Type::Path(path))) => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .ok_or_else(|| anyhow!("empty metric value type"))?,
        _ => bail!("unsupported value type for metric {rust_instrument}"),
    };
    let (instrument, temporality) = match rust_instrument.as_str() {
        "Counter" => ("counter", Some("delta")),
        "ObserveCounter" => ("counter", Some("cumulative")),
        "UpDownCounter" | "ObserveUpDownCounter" => ("updowncounter", Some("cumulative")),
        "Gauge" => ("gauge", None),
        _ => bail!("unsupported metric instrument {rust_instrument}"),
    };
    Ok(MetricShape {
        instrument: instrument.to_owned(),
        rust_instrument,
        value_type,
        temporality: temporality.map(str::to_owned),
    })
}

fn cargo_targets(workspace: &Path) -> Result<Vec<Target>> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version=1", "--no-deps"])
        .current_dir(workspace)
        .output()
        .context("failed to execute cargo metadata")?;
    if !output.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let workspace_root = PathBuf::from(
        metadata["workspace_root"]
            .as_str()
            .ok_or_else(|| anyhow!("cargo metadata omitted workspace_root"))?,
    )
    .canonicalize()?;
    let workspace_members = metadata["workspace_members"]
        .as_array()
        .ok_or_else(|| anyhow!("cargo metadata omitted workspace_members"))?
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<HashSet<_>>();

    let mut targets = Vec::new();
    for package in metadata["packages"]
        .as_array()
        .ok_or_else(|| anyhow!("cargo metadata omitted packages"))?
    {
        let Some(package_id) = package["id"].as_str() else {
            continue;
        };
        if !workspace_members.contains(package_id) {
            continue;
        }
        let package_name = package["name"]
            .as_str()
            .ok_or_else(|| anyhow!("package omitted name"))?;
        if !package_is_source_inventory(package) {
            continue;
        }
        for target in package["targets"]
            .as_array()
            .ok_or_else(|| anyhow!("package {package_name} omitted targets"))?
        {
            let kinds = target["kind"]
                .as_array()
                .ok_or_else(|| anyhow!("target omitted kind"))?;
            let production = kinds
                .iter()
                .filter_map(serde_json::Value::as_str)
                .any(|kind| {
                    matches!(
                        kind,
                        "lib" | "rlib" | "dylib" | "cdylib" | "proc-macro" | "bin"
                    )
                });
            if !production {
                continue;
            }
            let source = PathBuf::from(
                target["src_path"]
                    .as_str()
                    .ok_or_else(|| anyhow!("target omitted src_path"))?,
            );
            if source.starts_with(&workspace_root) {
                targets.push(Target {
                    package: package_name.to_owned(),
                    source,
                });
            }
        }
    }
    targets
        .sort_by(|left, right| (&left.package, &left.source).cmp(&(&right.package, &right.source)));
    targets.dedup_by(|left, right| left.package == right.package && left.source == right.source);
    Ok(targets)
}

fn package_is_source_inventory(package: &serde_json::Value) -> bool {
    package
        .pointer("/metadata/semconv/source_inventory")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true)
}

fn resolve_module(
    source_path: &Path,
    module_dir: &Path,
    module: &syn::ItemMod,
) -> Result<(PathBuf, PathBuf)> {
    if let Some(path) = path_attribute(&module.attrs)? {
        let source = source_path.parent().unwrap_or(module_dir).join(path);
        let child_dir = source
            .parent()
            .ok_or_else(|| anyhow!("module path has no parent: {}", source.display()))?
            .to_path_buf();
        return Ok((source, child_dir));
    }
    let name = module.ident.to_string();
    let direct = module_dir.join(format!("{name}.rs"));
    let nested = module_dir.join(&name).join("mod.rs");
    let source = if direct.is_file() {
        direct
    } else if nested.is_file() {
        nested
    } else {
        bail!(
            "cannot resolve module {} from {}",
            module.ident,
            module_dir.display()
        );
    };
    Ok((source, module_dir.join(name)))
}

fn path_attribute(attrs: &[Attribute]) -> Result<Option<PathBuf>> {
    for attr in attrs {
        if !attr.path().is_ident("path") {
            continue;
        }
        let Meta::NameValue(value) = &attr.meta else {
            bail!("path attribute must be a name-value pair");
        };
        let Expr::Lit(ExprLit {
            lit: Lit::Str(path),
            ..
        }) = &value.value
        else {
            bail!("path attribute must contain a string literal");
        };
        return Ok(Some(PathBuf::from(path.value())));
    }
    Ok(None)
}

fn item_attributes(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        Item::Verbatim(_) => &[],
        _ => &[],
    }
}

fn combined_guards(parent: &[String], attrs: &[Attribute]) -> Result<Vec<String>> {
    let mut guards = parent.to_vec();
    for attr in attrs {
        if attr.path().is_ident("cfg") {
            let meta: Meta = attr.parse_args()?;
            guards.push(meta.to_token_stream().to_string());
        }
    }
    Ok(guards)
}

fn guard_string(guards: &[String]) -> Option<String> {
    (!guards.is_empty()).then(|| guards.join(" && "))
}

fn is_test_only(attrs: &[Attribute]) -> Result<bool> {
    if attrs.iter().any(|attr| attr.path().is_ident("test")) {
        return Ok(true);
    }
    for attr in attrs {
        if !attr.path().is_ident("cfg") {
            continue;
        }
        let meta: Meta = attr.parse_args()?;
        if evaluate_production_cfg(&meta)? == Truth::False {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Truth {
    False,
    Unknown,
    True,
}

fn evaluate_production_cfg(meta: &Meta) -> Result<Truth> {
    match meta {
        Meta::Path(path) if path.is_ident("test") => Ok(Truth::False),
        Meta::Path(_) => Ok(Truth::Unknown),
        Meta::NameValue(value) => {
            if value.path.is_ident("feature")
                && matches!(
                    &value.value,
                    Expr::Lit(ExprLit { lit: Lit::Str(value), .. }) if value.value() == "test-utils"
                )
            {
                Ok(Truth::False)
            } else {
                Ok(Truth::Unknown)
            }
        }
        Meta::List(list) if list.path.is_ident("all") => {
            let children = parse_meta_list(list)?;
            let mut result = Truth::True;
            for child in children {
                match evaluate_production_cfg(&child)? {
                    Truth::False => return Ok(Truth::False),
                    Truth::Unknown => result = Truth::Unknown,
                    Truth::True => {}
                }
            }
            Ok(result)
        }
        Meta::List(list) if list.path.is_ident("any") => {
            let children = parse_meta_list(list)?;
            let mut result = Truth::False;
            for child in children {
                match evaluate_production_cfg(&child)? {
                    Truth::True => return Ok(Truth::True),
                    Truth::Unknown => result = Truth::Unknown,
                    Truth::False => {}
                }
            }
            Ok(result)
        }
        Meta::List(list) if list.path.is_ident("not") => {
            let children = parse_meta_list(list)?;
            if children.len() != 1 {
                bail!("cfg(not(...)) must have one predicate");
            }
            Ok(match evaluate_production_cfg(&children[0])? {
                Truth::False => Truth::True,
                Truth::Unknown => Truth::Unknown,
                Truth::True => Truth::False,
            })
        }
        Meta::List(_) => Ok(Truth::Unknown),
    }
}

fn parse_meta_list(list: &syn::MetaList) -> Result<Vec<Meta>> {
    Ok(Punctuated::<Meta, Token![,]>::parse_terminated
        .parse2(list.tokens.clone())?
        .into_iter()
        .collect())
}

fn find_attr<'a>(attrs: &'a [Attribute], name: &str) -> Option<&'a Attribute> {
    attrs.iter().find(|attr| {
        attr.path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == name)
    })
}

fn required_name_value(attr: &Attribute, name: &str) -> Result<String> {
    optional_name_value(attr, name)?.ok_or_else(|| {
        anyhow!(
            "attribute {} is missing required {name} = \"...\"",
            attr.path().to_token_stream()
        )
    })
}

fn optional_name_value(attr: &Attribute, name: &str) -> Result<Option<String>> {
    let mut found = None;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident(name) {
            let value = meta.value()?.parse::<syn::LitStr>()?;
            found = Some(value.value());
        } else if meta.input.peek(Token![=]) {
            let _ = meta.value()?.parse::<Expr>()?;
        } else if meta.input.peek(syn::token::Paren) {
            meta.parse_nested_meta(|nested| {
                if nested.input.peek(Token![=]) {
                    let _ = nested.value()?.parse::<Expr>()?;
                }
                Ok(())
            })?;
        }
        Ok(())
    })?;
    Ok(found)
}

fn optional_type_value(attr: &Attribute, name: &str) -> Result<Option<String>> {
    let mut found = None;
    attr.parse_nested_meta(|meta| {
        if meta.path.is_ident(name) {
            let value = meta.value()?.parse::<Type>()?;
            let Type::Path(path) = value else {
                return Err(meta.error(format!("{name} must be a type path")));
            };
            found = path
                .path
                .segments
                .last()
                .map(|segment| segment.ident.to_string());
        } else if meta.input.peek(Token![=]) {
            let _ = meta.value()?.parse::<Expr>()?;
        }
        Ok(())
    })?;
    Ok(found)
}

fn parse_attribute_key(attr: &Attribute) -> Result<String> {
    parse_string_name_value_attr(attr, "attribute_key")
}

fn parse_attribute_value(attr: &Attribute) -> Result<String> {
    parse_string_name_value_attr(attr, "attribute_value")
}

fn parse_string_name_value_attr(attr: &Attribute, name: &str) -> Result<String> {
    let Meta::NameValue(value) = &attr.meta else {
        bail!("{name} must be a name-value attribute");
    };
    let Expr::Lit(ExprLit {
        lit: Lit::Str(key), ..
    }) = &value.value
    else {
        bail!("{name} value must be a string literal");
    };
    Ok(key.value())
}

fn doc_brief(attrs: &[Attribute]) -> String {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .filter_map(|attr| match &attr.meta {
            Meta::NameValue(value) => match &value.value {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(line),
                    ..
                }) => Some(line.value().trim().to_owned()),
                _ => None,
            },
            _ => None,
        })
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn type_name(ty: &Type) -> Result<String> {
    let Type::Path(path) = ty else {
        bail!("expected a path type, got {}", ty.to_token_stream());
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
        .ok_or_else(|| anyhow!("empty type path"))
}

fn primitive_attribute_type(ty: &Type) -> Option<&'static str> {
    if let Type::Reference(reference) = ty {
        return primitive_attribute_type(&reference.elem);
    }
    let name = type_name(ty).ok()?;
    Some(match name.as_str() {
        "String" | "str" | "Cow" | "PathBuf" => "string",
        "bool" => "boolean",
        "f32" | "f64" => "double",
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64" | "u128"
        | "usize" => "int",
        _ => return None,
    })
}

fn derives_trait(attrs: &[Attribute], trait_name: &str) -> Result<bool> {
    let mut found = false;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("derive")) {
        attr.parse_nested_meta(|meta| {
            if meta
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == trait_name)
            {
                found = true;
            }
            Ok(())
        })?;
    }
    Ok(found)
}

fn string_array(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::Reference(reference) => string_array(&reference.expr),
        Expr::Paren(paren) => string_array(&paren.expr),
        Expr::Group(group) => string_array(&group.expr),
        Expr::Array(array) => array
            .elems
            .iter()
            .map(|element| match element {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(value),
                    ..
                }) => Some(value.value()),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

fn enum_member_id(value: &str) -> String {
    let mut id = String::with_capacity(value.len());
    let mut previous_was_separator = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            id.push(character.to_ascii_lowercase());
            previous_was_separator = false;
        } else if !id.is_empty() && !previous_was_separator {
            id.push('_');
            previous_was_separator = true;
        }
    }
    while id.ends_with('_') {
        id.pop();
    }
    if id.is_empty() {
        "value".to_owned()
    } else {
        id
    }
}

fn to_snake_case(ident: &str) -> String {
    let mut result = String::with_capacity(ident.len() + 4);
    let mut previous_lower_or_digit = false;
    for character in ident.chars() {
        if character.is_uppercase() {
            if previous_lower_or_digit {
                result.push('_');
            }
            result.extend(character.to_lowercase());
            previous_lower_or_digit = false;
        } else {
            result.push(character);
            previous_lower_or_digit = character.is_lowercase() || character.is_ascii_digit();
        }
    }
    result
}

fn display_source(workspace: &Path, path: &Path) -> String {
    let canonical_workspace = workspace
        .canonicalize()
        .unwrap_or_else(|_| workspace.to_path_buf());
    path.strip_prefix(canonical_workspace)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn resolve_string_expr(
    expr: &Expr,
    constants: &BTreeMap<String, BTreeSet<String>>,
) -> Option<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Some(value.value()),
        Expr::Path(path) => {
            let name = path.path.segments.last()?.ident.to_string();
            let values = constants.get(&name)?;
            (values.len() == 1)
                .then(|| values.first().cloned())
                .flatten()
        }
        Expr::Macro(ExprMacro { mac, .. }) if mac.path.is_ident("concat") => {
            let args = Punctuated::<Expr, Token![,]>::parse_terminated
                .parse2(mac.tokens.clone())
                .ok()?;
            let mut value = String::new();
            for arg in args {
                value.push_str(&resolve_string_expr(&arg, constants)?);
            }
            Some(value)
        }
        Expr::Paren(paren) => resolve_string_expr(&paren.expr, constants),
        Expr::Group(group) => resolve_string_expr(&group.expr, constants),
        _ => None,
    }
}

fn parse_manual_descriptor(expr: &ExprStruct) -> Result<AttributeSetDefinition> {
    let descriptor = string_field(&expr.fields, "name")?.unwrap_or_default();
    let mut definition = AttributeSetDefinition {
        descriptor,
        ..AttributeSetDefinition::default()
    };
    let Some(fields_expr) = expression_field(&expr.fields, "fields") else {
        return Ok(definition);
    };
    let Expr::Reference(reference) = fields_expr else {
        return Ok(definition);
    };
    let Expr::Array(ExprArray { elems, .. }) = reference.expr.as_ref() else {
        return Ok(definition);
    };
    for elem in elems {
        let Expr::Struct(field) = elem else {
            continue;
        };
        let key = string_field(&field.fields, "key")?.unwrap_or_default();
        let brief = string_field(&field.fields, "brief")?.unwrap_or_default();
        let r#type = expression_field(&field.fields, "r#type")
            .or_else(|| expression_field(&field.fields, "type"))
            .and_then(last_path_ident)
            .map(|value| match value.as_str() {
                "String" => "string",
                "Boolean" => "boolean",
                "Int" => "int",
                "Double" => "double",
                _ => "any",
            })
            .unwrap_or("any")
            .to_owned();
        definition.fields.push(AttributeFieldDefinition {
            key,
            brief,
            r#type: AttributeType::Primitive(r#type),
            rust_field: String::new(),
            availability: None,
        });
    }
    Ok(definition)
}

fn expression_field<'a>(
    fields: &'a Punctuated<FieldValue, Token![,]>,
    name: &str,
) -> Option<&'a Expr> {
    fields.iter().find_map(|field| {
        let syn::Member::Named(member) = &field.member else {
            return None;
        };
        (member == name).then_some(&field.expr)
    })
}

fn string_field(fields: &Punctuated<FieldValue, Token![,]>, name: &str) -> Result<Option<String>> {
    let Some(expr) = expression_field(fields, name) else {
        return Ok(None);
    };
    let Expr::Lit(ExprLit {
        lit: Lit::Str(value),
        ..
    }) = expr
    else {
        bail!("descriptor field {name} must be a string literal");
    };
    Ok(Some(value.value()))
}

fn last_path_ident(expr: &Expr) -> Option<String> {
    let Expr::Path(path) = expr else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn component_scope_target(items: &[Item]) -> Result<Option<String>> {
    let mut target = None;
    for item in items {
        let Item::Macro(item_macro) = item else {
            continue;
        };
        if is_test_only(&item_macro.attrs)?
            || item_macro
                .mac
                .path
                .segments
                .last()
                .is_none_or(|segment| segment.ident != "otel_component_scope")
        {
            continue;
        }

        let arguments = Punctuated::<Meta, Token![,]>::parse_terminated
            .parse2(item_macro.mac.tokens.clone())?;
        let scope = arguments
            .iter()
            .find_map(|argument| {
                let Meta::NameValue(name_value) = argument else {
                    return None;
                };
                if !name_value.path.is_ident("target") {
                    return None;
                }
                Some(&name_value.value)
            })
            .ok_or_else(|| anyhow!("otel_component_scope! is missing its target argument"))?;
        let Expr::Lit(ExprLit {
            lit: Lit::Str(scope),
            ..
        }) = scope
        else {
            bail!("otel_component_scope! target must be a string literal");
        };
        if target.replace(scope.value()).is_some() {
            bail!("a Rust module must not declare multiple component telemetry scopes");
        }
    }
    Ok(target)
}

fn is_event_macro(macro_name: &str) -> bool {
    matches!(
        macro_name,
        "otel_info" | "otel_warn" | "otel_debug" | "otel_error" | "otel_event" | "raw_error"
    )
}

fn is_transparent_event_macro(macro_name: &str) -> bool {
    matches!(macro_name, "select" | "join" | "try_join")
}

struct EventVisitor<'a> {
    inventory: &'a mut Inventory,
    package: &'a str,
    event_scope: &'a str,
    source: &'a str,
    availability: Option<String>,
    errors: Vec<String>,
}

impl EventVisitor<'_> {
    fn scan_macro(&mut self, mac: &syn::Macro) {
        let Some(macro_name) = mac
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
        else {
            return;
        };
        if is_event_macro(&macro_name) {
            self.scan_event_macro(&macro_name, mac.tokens.clone(), mac.path.segments.len() > 1);
            return;
        }
        if is_transparent_event_macro(&macro_name) {
            self.scan_nested_event_macros(mac.tokens.clone());
        }
    }

    fn scan_event_macro(&mut self, macro_name: &str, tokens: TokenStream, qualified: bool) {
        match parse_event_macro(macro_name, tokens, &self.inventory.constants) {
            Ok(parsed) => {
                let scope = parsed.target.as_deref().unwrap_or_else(|| {
                    if qualified || macro_name == "raw_error" {
                        self.package
                    } else {
                        self.event_scope
                    }
                });
                let event = self
                    .inventory
                    .events
                    .entry(parsed.name.clone())
                    .or_insert_with(|| EventDefinition {
                        name: parsed.name.clone(),
                        ..EventDefinition::default()
                    });
                event.callsite_count += 1;
                for (key, value_type) in parsed.attributes {
                    *event.attribute_occurrences.entry(key.clone()).or_default() += 1;
                    event
                        .attributes
                        .entry(key)
                        .and_modify(|current| merge_primitive_type(current, &value_type))
                        .or_insert(value_type);
                }
                event.scopes.insert(scope.to_owned());
                event.severities.insert(parsed.severity);
                event.sources.insert(self.source.to_owned());
                if let Some(availability) = &self.availability {
                    event.availability.insert(availability.clone());
                }
            }
            Err(error) => self.errors.push(format!(
                "{}: failed to inventory {macro_name}! invocation: {error}",
                self.source
            )),
        }
    }

    fn scan_nested_event_macros(&mut self, tokens: TokenStream) {
        let trees = tokens.into_iter().collect::<Vec<_>>();
        let mut index = 0;
        while index < trees.len() {
            if let TokenTree::Ident(ident) = &trees[index]
                && is_event_macro(&ident.to_string())
                && matches!(trees.get(index + 1), Some(TokenTree::Punct(punct)) if punct.as_char() == '!')
                && let Some(TokenTree::Group(group)) = trees.get(index + 2)
            {
                let qualified = index >= 2
                    && matches!(&trees[index - 1], TokenTree::Punct(punct) if punct.as_char() == ':')
                    && matches!(&trees[index - 2], TokenTree::Punct(punct) if punct.as_char() == ':');
                self.scan_event_macro(&ident.to_string(), group.stream(), qualified);
                index += 3;
                continue;
            }
            if let TokenTree::Group(group) = &trees[index] {
                self.scan_nested_event_macros(group.stream());
            }
            index += 1;
        }
    }
}

impl<'ast> Visit<'ast> for EventVisitor<'_> {
    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.scan_macro(mac);
        visit::visit_macro(self, mac);
    }
}

#[derive(Debug)]
struct ParsedEvent {
    name: String,
    severity: String,
    attributes: BTreeMap<String, String>,
    target: Option<String>,
}

fn parse_event_macro(
    macro_name: &str,
    tokens: TokenStream,
    constants: &BTreeMap<String, BTreeSet<String>>,
) -> Result<ParsedEvent> {
    let segments = split_top_level_commas(tokens);
    let target = segments.first().and_then(parse_event_target).transpose()?;
    let name_index = usize::from(target.is_some()) + usize::from(macro_name == "otel_event");
    let name_tokens = segments
        .get(name_index)
        .ok_or_else(|| anyhow!("event macro is missing its event name"))?;
    let name_expr: Expr = syn::parse2(name_tokens.clone())?;
    let name = resolve_string_expr(&name_expr, constants).ok_or_else(|| {
        anyhow!(
            "event name is not a resolvable string literal or unique string constant: {}",
            name_tokens
        )
    })?;
    let severity = match macro_name {
        "otel_info" => "info",
        "otel_warn" => "warn",
        "otel_debug" => "debug",
        "otel_error" | "raw_error" => "error",
        "otel_event" => "dynamic",
        _ => unreachable!(),
    }
    .to_owned();

    let mut attributes = BTreeMap::new();
    let mut body_started = false;
    for segment in segments.iter().skip(name_index + 1) {
        if segment.is_empty() {
            continue;
        }
        if starts_with_string_literal(segment) {
            body_started = true;
            continue;
        }
        if body_started {
            continue;
        }
        if let Some((key, value_type)) = parse_event_field(segment)? {
            if key != "message" {
                attributes
                    .entry(key)
                    .and_modify(|current| merge_primitive_type(current, &value_type))
                    .or_insert(value_type);
            }
        }
    }
    Ok(ParsedEvent {
        name,
        severity,
        attributes,
        target,
    })
}

fn parse_event_target(tokens: &TokenStream) -> Option<Result<String>> {
    let mut trees = tokens.clone().into_iter();
    let Some(TokenTree::Ident(target)) = trees.next() else {
        return None;
    };
    let Some(TokenTree::Punct(colon)) = trees.next() else {
        return None;
    };
    if target != "target" || colon.as_char() != ':' {
        return None;
    }
    let expression = trees.collect::<TokenStream>();
    Some(
        syn::parse2::<Expr>(expression)
            .map_err(anyhow::Error::from)
            .and_then(|expression| {
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(value),
                    ..
                }) = expression
                else {
                    bail!("event macro target must be a string literal");
                };
                Ok(value.value())
            }),
    )
}

fn split_top_level_commas(tokens: TokenStream) -> Vec<TokenStream> {
    let mut segments = vec![TokenStream::new()];
    for token in tokens {
        if matches!(&token, TokenTree::Punct(punct) if punct.as_char() == ',') {
            segments.push(TokenStream::new());
        } else if let Some(segment) = segments.last_mut() {
            segment.extend([token]);
        }
    }
    segments
}

fn starts_with_string_literal(tokens: &TokenStream) -> bool {
    matches!(tokens.clone().into_iter().next(), Some(TokenTree::Literal(literal)) if literal.to_string().starts_with('"'))
}

fn parse_event_field(tokens: &TokenStream) -> Result<Option<(String, String)>> {
    let token_vec = tokens.clone().into_iter().collect::<Vec<_>>();
    if let Some(eq_index) = token_vec
        .iter()
        .position(|token| matches!(token, TokenTree::Punct(punct) if punct.as_char() == '='))
    {
        let key = field_key(&token_vec[..eq_index])?;
        let value_type = field_value_type(&token_vec[eq_index + 1..]);
        return Ok(Some((key, value_type)));
    }

    let mut iter = token_vec.iter();
    let first = iter.next();
    let (formatted, ident) = match first {
        Some(TokenTree::Punct(punct)) if matches!(punct.as_char(), '?' | '%') => {
            (true, iter.next())
        }
        other => (false, other),
    };
    if let Some(TokenTree::Ident(ident)) = ident {
        return Ok(Some((
            ident.to_string().trim_start_matches("r#").to_owned(),
            if formatted { "string" } else { "any" }.to_owned(),
        )));
    }
    bail!("unsupported tracing field syntax: {tokens}")
}

fn field_key(tokens: &[TokenTree]) -> Result<String> {
    match tokens {
        [TokenTree::Ident(ident)] => Ok(ident.to_string().trim_start_matches("r#").to_owned()),
        [TokenTree::Literal(literal)] => {
            let literal: syn::LitStr = syn::parse_str(&literal.to_string())?;
            Ok(literal.value())
        }
        _ => bail!(
            "unsupported tracing field key: {}",
            tokens.iter().cloned().collect::<TokenStream>()
        ),
    }
}

fn field_value_type(tokens: &[TokenTree]) -> String {
    let Some(first) = tokens.first() else {
        return "any".to_owned();
    };
    match first {
        TokenTree::Punct(punct) if matches!(punct.as_char(), '?' | '%') => "string".to_owned(),
        TokenTree::Literal(literal) => {
            let value = literal.to_string();
            if value.starts_with('"') || value.starts_with("r\"") || value.starts_with("r#") {
                "string"
            } else if matches!(value.as_str(), "true" | "false") {
                "boolean"
            } else if value.contains('.') {
                "double"
            } else {
                "int"
            }
            .to_owned()
        }
        TokenTree::Ident(ident) if matches!(ident.to_string().as_str(), "true" | "false") => {
            "boolean".to_owned()
        }
        _ => "any".to_owned(),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricSetCatalogFile {
    params: MetricSetCatalogParams,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricSetCatalogParams {
    schema: String,
    metric_sets: Vec<MetricSetSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricSetSpec {
    id: String,
    #[serde(default)]
    availability: Option<String>,
    rust: MetricSetRustSpec,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricSetRustSpec {
    package: String,
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    types: Vec<MetricSetRustTypeSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricSetRustTypeSpec {
    r#type: String,
    metrics: BTreeSet<String>,
}

#[derive(Debug, Default)]
struct MetricSetCatalog {
    metric_sets: BTreeMap<String, MetricSetSpec>,
}

impl MetricSetCatalog {
    fn load(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read metric-set catalog {}", path.display()))?;
        let catalog: MetricSetCatalogFile = serde_yaml::from_str(&contents)
            .with_context(|| format!("failed to parse metric-set catalog {}", path.display()))?;
        if catalog.params.schema != "otap-dataflow/metric-sets/1" {
            bail!(
                "{} uses schema {}, expected otap-dataflow/metric-sets/1",
                path.display(),
                catalog.params.schema
            );
        }

        let mut metric_sets = BTreeMap::new();
        for metric_set in catalog.params.metric_sets {
            let id = metric_set.id.clone();
            if metric_sets.insert(id.clone(), metric_set).is_some() {
                bail!("duplicate metric-set definition {id} in {}", path.display());
            }
        }
        Ok(Self { metric_sets })
    }

    fn rust_types_for(&self, metric_set: &str, metric: &str) -> BTreeSet<String> {
        let Some(spec) = self.metric_sets.get(metric_set) else {
            return BTreeSet::new();
        };
        if let Some(rust_type) = &spec.rust.r#type {
            return [rust_type.clone()].into_iter().collect();
        }
        spec.rust
            .types
            .iter()
            .filter(|rust_type| rust_type.metrics.contains(metric))
            .map(|rust_type| rust_type.r#type.clone())
            .collect()
    }

    fn check(&self, inventory: &Inventory, errors: &mut Vec<String>) {
        let expected_sets = inventory
            .metrics
            .values()
            .map(|metric| metric.metric_set.clone())
            .collect::<BTreeSet<_>>();
        compare_keys(
            "metric set",
            expected_sets.iter(),
            self.metric_sets.keys(),
            errors,
        );

        for (id, spec) in &self.metric_sets {
            let has_single_type = spec.rust.r#type.is_some();
            let has_split_types = !spec.rust.types.is_empty();
            if has_single_type == has_split_types {
                errors.push(format!(
                    "metric set {id} must define exactly one of rust.type or rust.types"
                ));
            }

            for rust_type in &spec.rust.types {
                if rust_type.metrics.is_empty() {
                    errors.push(format!(
                        "metric set {id} Rust type {} has no metrics",
                        rust_type.r#type
                    ));
                }
                for metric in &rust_type.metrics {
                    match inventory.metrics.get(metric) {
                        Some(definition) if definition.metric_set == *id => {}
                        Some(definition) => errors.push(format!(
                            "metric set {id} assigns metric {metric} from set {}",
                            definition.metric_set
                        )),
                        None => errors.push(format!(
                            "metric set {id} assigns unknown metric {metric} to Rust type {}",
                            rust_type.r#type
                        )),
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct Registry {
    attributes: BTreeMap<String, RegistryAttribute>,
    entities: BTreeMap<String, RegistryEntity>,
    metrics: BTreeMap<String, RegistryMetric>,
    events: BTreeMap<String, RegistryEvent>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DefinitionFile {
    file_format: String,
    #[serde(default)]
    attributes: Vec<RegistryAttribute>,
    #[serde(default)]
    entities: Vec<RegistryEntity>,
    #[serde(default)]
    metrics: Vec<RegistryMetric>,
    #[serde(default)]
    events: Vec<RegistryEvent>,
}

#[derive(Debug, Clone, Deserialize)]
struct RegistryAttribute {
    key: String,
    r#type: AttributeType,
    brief: String,
    stability: String,
    #[serde(default)]
    annotations: RegistryAnnotations,
}

impl RegistryAttribute {
    fn wire_key(&self) -> &str {
        self.annotations
            .otap_dataflow
            .wire
            .as_ref()
            .and_then(|wire| wire.attribute_key.as_deref())
            .unwrap_or(&self.key)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RegistryEntity {
    r#type: String,
    brief: String,
    stability: String,
    requirement_level: String,
    identity: Vec<RegistryAttributeRef>,
    annotations: RegistryAnnotations,
}

#[derive(Debug, Clone, Deserialize)]
struct RegistryMetric {
    name: String,
    brief: String,
    instrument: String,
    unit: String,
    stability: String,
    requirement_level: String,
    #[serde(default)]
    attributes: Vec<RegistryAttributeRef>,
    entity_associations: Vec<serde_yaml::Value>,
    annotations: RegistryAnnotations,
}

#[derive(Debug, Clone, Deserialize)]
struct RegistryEvent {
    name: String,
    brief: String,
    stability: String,
    requirement_level: String,
    #[serde(default)]
    attributes: Vec<RegistryAttributeRef>,
    entity_associations: Vec<serde_yaml::Value>,
    annotations: RegistryAnnotations,
}

impl RegistryEvent {
    fn wire_name(&self) -> &str {
        self.annotations
            .otap_dataflow
            .wire
            .as_ref()
            .and_then(|wire| wire.event_name.as_deref())
            .unwrap_or(&self.name)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RegistryAttributeRef {
    r#ref: String,
    #[serde(default)]
    requirement_level: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RegistryAnnotations {
    #[serde(default)]
    code_generation: Option<CodeGenerationAnnotation>,
    #[serde(default)]
    otap_dataflow: OtapDataflowAnnotation,
}

#[derive(Debug, Clone, Deserialize)]
struct CodeGenerationAnnotation {
    metric_value_type: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct OtapDataflowAnnotation {
    #[serde(default)]
    metric_set: Option<String>,
    #[serde(default)]
    scope_descriptor: Option<String>,
    #[serde(default)]
    parent_entities: Vec<String>,
    #[serde(default)]
    dynamic_identity: bool,
    #[serde(default)]
    recording: Option<String>,
    #[serde(default)]
    availability: Option<String>,
    #[serde(default)]
    rust: Option<RustAnnotation>,
    #[serde(default)]
    wire: Option<WireAnnotation>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RustAnnotation {
    #[serde(default)]
    package: Option<String>,
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    field: Option<String>,
    #[serde(default)]
    instrument: Option<String>,
    #[serde(default)]
    value_type: Option<String>,
    #[serde(default)]
    temporality: Option<String>,
    #[serde(default)]
    availability: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct WireAnnotation {
    #[serde(default)]
    attribute_key: Option<String>,
    #[serde(default)]
    event_name: Option<String>,
    #[serde(default)]
    scope_names: BTreeSet<String>,
    #[serde(default)]
    severity_levels: BTreeSet<String>,
    #[serde(default)]
    sources: BTreeSet<String>,
    #[serde(default)]
    availability: BTreeSet<String>,
}

struct EntitySpec {
    r#type: &'static str,
    descriptor: &'static str,
    parent: Option<&'static str>,
    dynamic_identity: bool,
}

const ENTITY_SPECS: &[EntitySpec] = &[
    EntitySpec {
        r#type: "otap.engine",
        descriptor: "engine",
        parent: None,
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.controller",
        descriptor: "controller.attrs",
        parent: Some("otap.engine"),
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.pipeline",
        descriptor: "pipeline.attrs",
        parent: Some("otap.controller"),
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.extension.scope",
        descriptor: "extension.scope.attrs",
        parent: Some("otap.pipeline"),
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.extension",
        descriptor: "extension.attrs",
        parent: Some("otap.extension.scope"),
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.node",
        descriptor: "node.attrs",
        parent: Some("otap.pipeline"),
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.node.custom",
        descriptor: "node.custom.attrs",
        parent: Some("otap.node"),
        dynamic_identity: true,
    },
    EntitySpec {
        r#type: "otap.node.topic",
        descriptor: "node.topic.attrs",
        parent: Some("otap.node"),
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.node.custom.topic",
        descriptor: "node.custom.topic.attrs",
        parent: Some("otap.node.custom"),
        dynamic_identity: true,
    },
    EntitySpec {
        r#type: "otap.node.channel",
        descriptor: "node.channel.attrs",
        parent: Some("otap.node"),
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.extension.channel",
        descriptor: "extension.channel.attrs",
        parent: Some("otap.extension"),
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.flow",
        descriptor: "flow.attrs",
        parent: Some("otap.pipeline"),
        dynamic_identity: false,
    },
    EntitySpec {
        r#type: "otap.controller.monitor",
        descriptor: "controller.monitor.attrs",
        parent: Some("otap.engine"),
        dynamic_identity: false,
    },
];

impl Registry {
    fn load(path: &Path) -> Result<Self> {
        if !path.is_dir() {
            bail!(
                "semantic-convention registry does not exist: {}",
                path.display()
            );
        }
        let registry_path = path.join("registry");
        let mut files = Vec::new();
        collect_yaml_files(&registry_path, &mut files)?;
        if files.is_empty() {
            bail!(
                "no v2 definition files found under {}",
                registry_path.display()
            );
        }
        files.sort();

        let mut registry = Self::default();
        for file in files {
            let contents = fs::read_to_string(&file)
                .with_context(|| format!("failed to read registry file {}", file.display()))?;
            let definitions: DefinitionFile = serde_yaml::from_str(&contents)
                .with_context(|| format!("failed to parse v2 registry file {}", file.display()))?;
            if definitions.file_format != "definition/2" {
                bail!(
                    "{} uses file_format {}, expected definition/2",
                    file.display(),
                    definitions.file_format
                );
            }
            for attribute in definitions.attributes {
                insert_unique(
                    &mut registry.attributes,
                    attribute.key.clone(),
                    attribute,
                    &file,
                )?;
            }
            for entity in definitions.entities {
                insert_unique(&mut registry.entities, entity.r#type.clone(), entity, &file)?;
            }
            for metric in definitions.metrics {
                insert_unique(&mut registry.metrics, metric.name.clone(), metric, &file)?;
            }
            for event in definitions.events {
                insert_unique(&mut registry.events, event.name.clone(), event, &file)?;
            }
        }
        Ok(registry)
    }

    fn check(&self, inventory: &Inventory, metric_sets: &MetricSetCatalog) -> Result<()> {
        let mut errors = Vec::new();
        self.check_attributes(inventory, &mut errors);
        self.check_entities(inventory, &mut errors);
        metric_sets.check(inventory, &mut errors);
        self.check_metrics(inventory, metric_sets, &mut errors);
        self.check_events(inventory, &mut errors);
        self.check_parent_cycles(&mut errors);
        if errors.is_empty() {
            Ok(())
        } else {
            errors.sort();
            bail!(
                "semantic-convention drift detected ({} findings):\n- {}",
                errors.len(),
                errors.join("\n- ")
            )
        }
    }

    fn check_attributes(&self, inventory: &Inventory, errors: &mut Vec<String>) {
        let expected = expected_attributes(inventory);
        let actual = self
            .attributes
            .values()
            .map(|attribute| (attribute.wire_key().to_owned(), attribute))
            .collect::<BTreeMap<_, _>>();
        compare_keys("attribute", expected.keys(), actual.keys(), errors);
        for (wire_key, expected_type) in expected {
            let Some(attribute) = actual.get(&wire_key) else {
                continue;
            };
            check_common(
                "attribute",
                &wire_key,
                &attribute.brief,
                &attribute.stability,
                None,
                errors,
            );
            check_equal(
                &format!("attribute {wire_key} type"),
                &attribute.r#type,
                &expected_type,
                errors,
            );
        }
    }

    fn check_entities(&self, inventory: &Inventory, errors: &mut Vec<String>) {
        let expected_types = ENTITY_SPECS
            .iter()
            .map(|spec| spec.r#type.to_owned())
            .collect::<BTreeSet<_>>();
        compare_keys(
            "entity",
            expected_types.iter(),
            self.entities.keys(),
            errors,
        );
        for spec in ENTITY_SPECS {
            let Some(entity) = self.entities.get(spec.r#type) else {
                continue;
            };
            check_common(
                "entity",
                spec.r#type,
                &entity.brief,
                &entity.stability,
                Some(&entity.requirement_level),
                errors,
            );
            let annotation = &entity.annotations.otap_dataflow;
            check_equal(
                &format!("entity {} scope_descriptor", spec.r#type),
                annotation.scope_descriptor.as_deref().unwrap_or_default(),
                spec.descriptor,
                errors,
            );
            let expected_parents = spec
                .parent
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            check_equal(
                &format!("entity {} parent_entities", spec.r#type),
                &annotation.parent_entities,
                &expected_parents,
                errors,
            );
            check_equal(
                &format!("entity {} dynamic_identity", spec.r#type),
                &annotation.dynamic_identity,
                &spec.dynamic_identity,
                errors,
            );
            let Some(set) = inventory
                .attribute_sets
                .values()
                .find(|set| set.descriptor == spec.descriptor)
            else {
                errors.push(format!(
                    "entity {} references missing source descriptor {}",
                    spec.r#type, spec.descriptor
                ));
                continue;
            };
            if let Some(rust) = &annotation.rust {
                check_rust_entity(spec.r#type, set, rust, errors);
            } else {
                errors.push(format!(
                    "entity {} is missing otap_dataflow.rust",
                    spec.r#type
                ));
            }
            let mut expected_identity = flatten_attribute_set(inventory, &set.rust_type, errors);
            expected_identity.insert("service.instance.id".to_owned());
            let actual_identity = entity
                .identity
                .iter()
                .filter_map(|reference| self.wire_attribute_key(&reference.r#ref))
                .collect::<BTreeSet<_>>();
            check_equal(
                &format!("entity {} flattened identity", spec.r#type),
                &actual_identity,
                &expected_identity,
                errors,
            );
        }
    }

    fn check_metrics(
        &self,
        inventory: &Inventory,
        metric_sets: &MetricSetCatalog,
        errors: &mut Vec<String>,
    ) {
        compare_keys(
            "metric",
            inventory.metrics.keys(),
            self.metrics.keys(),
            errors,
        );
        for (name, expected) in &inventory.metrics {
            let Some(metric) = self.metrics.get(name) else {
                continue;
            };
            check_common(
                "metric",
                name,
                &metric.brief,
                &metric.stability,
                Some(&metric.requirement_level),
                errors,
            );
            check_equal(
                &format!("metric {name} instrument"),
                &metric.instrument,
                &expected.instrument,
                errors,
            );
            check_equal(
                &format!("metric {name} unit"),
                &metric.unit,
                &expected.unit,
                errors,
            );
            let annotation = &metric.annotations.otap_dataflow;
            check_equal(
                &format!("metric {name} metric_set annotation"),
                annotation.metric_set.as_deref().unwrap_or_default(),
                &expected.metric_set,
                errors,
            );
            let expected_codegen = if expected.value_type == "f64" {
                "double"
            } else {
                "int"
            };
            check_equal(
                &format!("metric {name} code_generation.metric_value_type"),
                metric
                    .annotations
                    .code_generation
                    .as_ref()
                    .map(|annotation| annotation.metric_value_type.as_str())
                    .unwrap_or_default(),
                expected_codegen,
                errors,
            );
            let actual_attributes = metric
                .attributes
                .iter()
                .filter_map(|attribute| self.wire_attribute_key(&attribute.r#ref))
                .collect::<BTreeSet<_>>();
            check_equal(
                &format!("metric {name} attributes"),
                &actual_attributes,
                &expected_metric_attributes(inventory, expected, errors),
                errors,
            );
            check_generated_metric_contract(
                name,
                expected,
                annotation,
                metric,
                metric_sets,
                errors,
            );
            self.check_associations(
                "metric",
                name,
                &metric.entity_associations,
                &expected_metric_entities(&expected.metric_set),
                errors,
            );
        }
    }

    fn check_events(&self, inventory: &Inventory, errors: &mut Vec<String>) {
        let global_attributes = expected_attributes(inventory);
        let mut by_wire = BTreeMap::new();
        for event in self.events.values() {
            let wire_name = event.wire_name();
            if by_wire.insert(wire_name.to_owned(), event).is_some() {
                errors.push(format!("wire event {wire_name} has multiple definitions"));
            }
        }
        compare_keys(
            "wire event",
            inventory.events.keys(),
            by_wire.keys(),
            errors,
        );
        for (wire_name, expected) in &inventory.events {
            let Some(event) = by_wire.get(wire_name) else {
                continue;
            };
            check_common(
                "event",
                &event.name,
                &event.brief,
                &event.stability,
                Some(&event.requirement_level),
                errors,
            );
            check_equal(
                &format!("wire event {wire_name} canonical name"),
                &event.name,
                &canonical_event_name(wire_name),
                errors,
            );
            let Some(wire) = event.annotations.otap_dataflow.wire.as_ref() else {
                errors.push(format!(
                    "event {wire_name} is missing otap_dataflow.wire metadata"
                ));
                continue;
            };
            check_equal(
                &format!("event {wire_name} scope_names"),
                &wire.scope_names,
                &expected.scopes,
                errors,
            );
            check_equal(
                &format!("event {wire_name} severity_levels"),
                &wire.severity_levels,
                &expected.severities,
                errors,
            );
            check_equal(
                &format!("event {wire_name} sources"),
                &wire.sources,
                &expected.sources,
                errors,
            );
            check_equal(
                &format!("event {wire_name} availability"),
                &wire.availability,
                &expected.availability,
                errors,
            );
            let mut actual_attributes = BTreeMap::new();
            for reference in &event.attributes {
                let Some(key) = self.wire_attribute_key(&reference.r#ref) else {
                    errors.push(format!(
                        "event {wire_name} references unknown local/upstream attribute {}",
                        reference.r#ref
                    ));
                    continue;
                };
                let value_type = self
                    .attributes
                    .get(&reference.r#ref)
                    .map(|attribute| attribute.r#type.wire_type().to_owned())
                    .unwrap_or_else(|| "upstream".to_owned());
                actual_attributes.insert(key.clone(), value_type);
                let expected_requirement = if expected
                    .attribute_occurrences
                    .get(&key)
                    .copied()
                    .unwrap_or_default()
                    == expected.callsite_count
                {
                    "required"
                } else {
                    "recommended"
                };
                check_equal(
                    &format!("event {wire_name} attribute {key} requirement_level"),
                    reference.requirement_level.as_deref().unwrap_or_default(),
                    expected_requirement,
                    errors,
                );
            }
            check_equal(
                &format!("event {wire_name} attributes"),
                &actual_attributes,
                &expected
                    .attributes
                    .keys()
                    .filter_map(|key| {
                        global_attributes
                            .get(key)
                            .map(|value_type| (key.clone(), value_type.wire_type().to_owned()))
                    })
                    .collect::<BTreeMap<_, _>>(),
                errors,
            );
            self.check_associations(
                "event",
                wire_name,
                &event.entity_associations,
                &expected_event_entities(expected),
                errors,
            );
        }
    }

    fn check_associations(
        &self,
        kind: &str,
        name: &str,
        expressions: &[serde_yaml::Value],
        expected: &BTreeSet<String>,
        errors: &mut Vec<String>,
    ) {
        let mut actual = BTreeSet::new();
        for expression in expressions {
            collect_association_entities(expression, &mut actual, errors, kind, name);
        }
        for entity in &actual {
            if !self.entities.contains_key(entity) {
                errors.push(format!("{kind} {name} associates unknown entity {entity}"));
            }
        }
        check_equal(
            &format!("{kind} {name} entity associations"),
            &actual,
            expected,
            errors,
        );
        if expected.len() > 1 && !has_explicit_one_of(expressions) {
            errors.push(format!(
                "{kind} {name} must express alternative entities with one_of"
            ));
        }
    }

    fn wire_attribute_key(&self, reference: &str) -> Option<String> {
        self.attributes.get(reference).map_or_else(
            || {
                matches!(
                    reference,
                    "service.instance.id" | "host.id" | "container.id"
                )
                .then(|| reference.to_owned())
            },
            |attribute| Some(attribute.wire_key().to_owned()),
        )
    }

    fn check_parent_cycles(&self, errors: &mut Vec<String>) {
        for entity in self.entities.keys() {
            let mut seen = BTreeSet::new();
            let mut current = Some(entity.as_str());
            while let Some(name) = current {
                if !seen.insert(name.to_owned()) {
                    errors.push(format!(
                        "entity parent cycle detected from {entity} through {name}"
                    ));
                    break;
                }
                current = self
                    .entities
                    .get(name)
                    .and_then(|entity| entity.annotations.otap_dataflow.parent_entities.first())
                    .map(String::as_str);
            }
        }
    }
}

fn collect_yaml_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read registry directory {}", path.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_yaml_files(&path, files)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "yaml")
        {
            files.push(path);
        }
    }
    Ok(())
}

fn insert_unique<T>(
    map: &mut BTreeMap<String, T>,
    key: String,
    value: T,
    file: &Path,
) -> Result<()> {
    if map.insert(key.clone(), value).is_some() {
        bail!(
            "duplicate definition {key} found while loading {}",
            file.display()
        );
    }
    Ok(())
}

fn expected_attributes(inventory: &Inventory) -> BTreeMap<String, AttributeType> {
    let mut expected = BTreeMap::new();
    for set in inventory.attribute_sets.values() {
        for field in &set.fields {
            merge_attribute_type(&mut expected, &field.key, &field.r#type);
        }
    }
    for event in inventory.events.values() {
        for (key, value_type) in &event.attributes {
            let observation = AttributeType::Primitive(value_type.clone());
            expected
                .entry(key.clone())
                .and_modify(|current| current.merge_observation(&observation))
                .or_insert(observation);
        }
    }
    expected
}

fn merge_attribute_type(
    attributes: &mut BTreeMap<String, AttributeType>,
    key: &str,
    value_type: &AttributeType,
) {
    attributes
        .entry(key.to_owned())
        .and_modify(|current| current.merge(value_type))
        .or_insert_with(|| value_type.clone());
}

fn merge_primitive_type(current: &mut String, incoming: &str) {
    if current != incoming {
        current.clear();
        current.push_str("any");
    }
}

fn flatten_attribute_set(
    inventory: &Inventory,
    rust_type: &str,
    errors: &mut Vec<String>,
) -> BTreeSet<String> {
    fn visit(
        inventory: &Inventory,
        rust_type: &str,
        visiting: &mut BTreeSet<String>,
        result: &mut BTreeSet<String>,
        errors: &mut Vec<String>,
    ) {
        if !visiting.insert(rust_type.to_owned()) {
            errors.push(format!(
                "Rust attribute-set composition cycle at {rust_type}"
            ));
            return;
        }
        let Some(set) = inventory.attribute_sets.get(rust_type) else {
            errors.push(format!("unknown Rust attribute set {rust_type}"));
            return;
        };
        result.extend(set.fields.iter().map(|field| field.key.clone()));
        for composed in &set.composes {
            visit(inventory, composed, visiting, result, errors);
        }
        visiting.remove(rust_type);
    }
    let mut result = BTreeSet::new();
    visit(
        inventory,
        rust_type,
        &mut BTreeSet::new(),
        &mut result,
        errors,
    );
    result
}

fn expected_metric_attributes(
    inventory: &Inventory,
    metric: &MetricDefinition,
    errors: &mut Vec<String>,
) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    let mut pending = metric
        .registration_attribute_sets
        .iter()
        .chain(&metric.measurement_attribute_sets)
        .cloned()
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    while let Some(rust_type) = pending.pop() {
        if !visited.insert(rust_type.clone()) {
            continue;
        }
        let mut found = false;
        for set in inventory
            .attribute_sets
            .values()
            .filter(|set| set.rust_type == rust_type)
        {
            found = true;
            result.extend(set.fields.iter().map(|field| field.key.clone()));
            pending.extend(set.composes.iter().cloned());
        }
        if !found {
            errors.push(format!(
                "metric {} references unknown attribute set {rust_type}",
                metric.canonical_name
            ));
        }
    }
    result
}

fn expected_metric_entities(metric_set: &str) -> BTreeSet<String> {
    let entities: &[&str] = match metric_set {
        "engine" => &["otap.engine"],
        "controller.monitor" => &["otap.controller.monitor"],
        "pipeline" | "pipeline.completion" | "pipeline.runtime_control" | "tokio.runtime" => {
            &["otap.pipeline"]
        }
        metric_set if metric_set.starts_with("extension.") => &["otap.extension"],
        "flow" => &["otap.flow"],
        "channel.receiver" | "channel.sender" => &["otap.node.channel", "otap.extension.channel"],
        "node.consumer" | "node.producer" => &["otap.node.channel"],
        "receiver.topic" | "exporter.topic" => &["otap.node.topic", "otap.node.custom.topic"],
        _ => &["otap.node", "otap.node.custom"],
    };
    entities.iter().map(|entity| (*entity).to_owned()).collect()
}

fn expected_event_entities(event: &EventDefinition) -> BTreeSet<String> {
    let name = event.name.as_str();
    let entities: &[&str] =
        if name.starts_with("channel.") {
            &["otap.node.channel", "otap.extension.channel"]
        } else if name.starts_with("flow.") {
            &["otap.flow"]
        } else if name.starts_with("topic_") || name.starts_with("topic.") {
            &["otap.node.topic", "otap.node.custom.topic"]
        } else if name.contains("extension")
            || event
                .sources
                .iter()
                .any(|source| source.contains("contrib-extensions"))
        {
            &["otap.extension"]
        } else if name.starts_with("pipeline.") || name.starts_with("tokio.metrics") {
            &["otap.pipeline"]
        } else if name.starts_with("engine.")
            || name.starts_with("controller.")
            || name.starts_with("tracing.")
        {
            &["otap.engine"]
        } else if event.sources.iter().any(|source| {
            source.contains("crates/engine/") || source.contains("crates/controller/")
        }) {
            &[
                "otap.engine",
                "otap.pipeline",
                "otap.node",
                "otap.node.custom",
            ]
        } else if event.sources.iter().any(|source| {
            source.contains("core-nodes")
                || source.contains("contrib-nodes")
                || source.contains("crates/otap/")
                || source.contains("crates/validation/")
                || source.contains("crates/quiver/")
        }) {
            &["otap.node", "otap.node.custom"]
        } else {
            &["otap.engine"]
        };
    entities.iter().map(|entity| (*entity).to_owned()).collect()
}

fn canonical_event_name(wire_name: &str) -> String {
    if wire_name
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_lowercase())
        && wire_name.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '_' | '.')
        })
    {
        return wire_name.to_owned();
    }
    let mut normalized = String::new();
    let mut previous_lower_or_digit = false;
    for character in wire_name.chars() {
        if character.is_ascii_uppercase() {
            if previous_lower_or_digit {
                normalized.push('_');
            }
            normalized.push(character.to_ascii_lowercase());
            previous_lower_or_digit = false;
        } else if character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '_' | '.')
        {
            normalized.push(character);
            previous_lower_or_digit = character.is_ascii_lowercase() || character.is_ascii_digit();
        } else if !normalized.ends_with('_') {
            normalized.push('_');
            previous_lower_or_digit = false;
        }
    }
    format!("otap.{normalized}")
}

fn collect_association_entities(
    value: &serde_yaml::Value,
    entities: &mut BTreeSet<String>,
    errors: &mut Vec<String>,
    kind: &str,
    name: &str,
) {
    match value {
        serde_yaml::Value::String(entity) => {
            entities.insert(entity.clone());
        }
        serde_yaml::Value::Mapping(mapping) => {
            for operator in ["one_of", "all_of"] {
                let key = serde_yaml::Value::String(operator.to_owned());
                if let Some(serde_yaml::Value::Sequence(children)) = mapping.get(&key) {
                    for child in children {
                        collect_association_entities(child, entities, errors, kind, name);
                    }
                    return;
                }
            }
            errors.push(format!(
                "{kind} {name} has an invalid entity association mapping"
            ));
        }
        _ => errors.push(format!(
            "{kind} {name} has an invalid entity association expression"
        )),
    }
}

fn has_explicit_one_of(expressions: &[serde_yaml::Value]) -> bool {
    expressions.iter().any(|expression| {
        let serde_yaml::Value::Mapping(mapping) = expression else {
            return false;
        };
        mapping.contains_key(serde_yaml::Value::String("one_of".to_owned()))
    })
}

fn check_common(
    kind: &str,
    name: &str,
    brief: &str,
    stability: &str,
    requirement_level: Option<&String>,
    errors: &mut Vec<String>,
) {
    if brief.trim().is_empty() {
        errors.push(format!("{kind} {name} has an empty brief"));
    }
    check_equal(
        &format!("{kind} {name} stability"),
        stability,
        "development",
        errors,
    );
    if let Some(requirement_level) = requirement_level {
        check_equal(
            &format!("{kind} {name} requirement_level"),
            requirement_level.as_str(),
            "recommended",
            errors,
        );
    }
}

fn check_rust_entity(
    entity: &str,
    expected: &AttributeSetDefinition,
    actual: &RustAnnotation,
    errors: &mut Vec<String>,
) {
    check_equal(
        &format!("entity {entity} rust.package"),
        actual.package.as_deref().unwrap_or_default(),
        expected.package.as_str(),
        errors,
    );
    check_equal(
        &format!("entity {entity} rust.type"),
        actual.r#type.as_deref().unwrap_or_default(),
        expected.rust_type.as_str(),
        errors,
    );
    check_equal(
        &format!("entity {entity} rust.source"),
        actual.source.as_deref().unwrap_or_default(),
        expected.source.as_str(),
        errors,
    );
    check_equal(
        &format!("entity {entity} rust.availability"),
        &actual.availability,
        &expected.availability,
        errors,
    );
}

fn default_recording_mode(instrument: &str) -> Option<&'static str> {
    match instrument {
        "counter" => Some("additive"),
        "updowncounter" => Some("observed"),
        _ => None,
    }
}

fn generated_metric_shape(
    instrument: &str,
    recording_override: Option<&str>,
) -> Option<(&'static str, Option<&'static str>)> {
    let recording = recording_override.or_else(|| default_recording_mode(instrument));
    match (instrument, recording) {
        ("counter", Some("additive")) => Some(("Counter", Some("delta"))),
        ("counter", Some("observed")) => Some(("ObserveCounter", Some("cumulative"))),
        ("updowncounter", Some("additive")) => Some(("UpDownCounter", Some("cumulative"))),
        ("updowncounter", Some("observed")) => Some(("ObserveUpDownCounter", Some("cumulative"))),
        ("gauge", None) => Some(("Gauge", None)),
        ("histogram", None) => Some(("Mmsc", Some("delta"))),
        _ => None,
    }
}

fn check_generated_metric_contract(
    metric: &str,
    expected: &MetricDefinition,
    annotation: &OtapDataflowAnnotation,
    definition: &RegistryMetric,
    metric_sets: &MetricSetCatalog,
    errors: &mut Vec<String>,
) {
    let Some(metric_set) = metric_sets.metric_sets.get(&expected.metric_set) else {
        return;
    };

    check_equal(
        &format!("metric set {} rust.package", expected.metric_set),
        metric_set.rust.package.as_str(),
        expected.package.as_str(),
        errors,
    );
    check_equal(
        &format!("metric {metric} derived rust.types"),
        &metric_sets.rust_types_for(&expected.metric_set, metric),
        &expected.rust_types,
        errors,
    );

    let rust = annotation.rust.as_ref();
    let derived_field = expected.name.replace('.', "_");
    let expected_field_override =
        (derived_field != expected.rust_field).then_some(expected.rust_field.as_str());
    check_equal(
        &format!("metric {metric} rust.field override"),
        &rust.and_then(|rust| rust.field.as_deref()),
        &expected_field_override,
        errors,
    );

    if let Some(rust) = rust {
        if rust.package.is_some()
            || rust.r#type.is_some()
            || rust.source.is_some()
            || rust.value_type.is_some()
            || rust.temporality.is_some()
            || rust.availability.is_some()
        {
            errors.push(format!(
                "metric {metric} contains redundant per-metric Rust metadata"
            ));
        }
        if rust.field.is_none() && rust.instrument.is_none() {
            errors.push(format!("metric {metric} has an empty rust override"));
        }
    }
    if annotation.wire.is_some() {
        errors.push(format!(
            "metric {metric} contains redundant wire metadata; derive it from the metric-set prefix"
        ));
    }

    let default_recording = default_recording_mode(&expected.instrument);
    if annotation.recording.is_some() && annotation.recording.as_deref() == default_recording {
        errors.push(format!(
            "metric {metric} redundantly declares the default {} recording mode",
            default_recording.unwrap_or_default()
        ));
    }
    let (derived_instrument, derived_temporality) =
        match generated_metric_shape(&expected.instrument, annotation.recording.as_deref()) {
            Some(shape) => shape,
            None => {
                errors.push(format!(
                "metric {metric} has unsupported generation mapping instrument={} recording={:?}",
                expected.instrument, annotation.recording
            ));
                ("", None)
            }
        };
    let instrument_override = rust.and_then(|rust| rust.instrument.as_deref());
    if instrument_override == Some(derived_instrument) {
        errors.push(format!(
            "metric {metric} redundantly declares the default Rust instrument {derived_instrument}"
        ));
    }
    if let Some(instrument_override) = instrument_override
        && (expected.instrument != "histogram"
            || !matches!(instrument_override, "HistogramNormal" | "HistogramDetailed"))
    {
        errors.push(format!(
            "metric {metric} has unsupported Rust instrument override {instrument_override}"
        ));
    }
    let effective_instrument = instrument_override.unwrap_or(derived_instrument);
    check_equal(
        &format!("metric {metric} effective Rust instrument"),
        effective_instrument,
        expected.rust_instrument.as_str(),
        errors,
    );
    check_equal(
        &format!("metric {metric} derived temporality"),
        &derived_temporality,
        &expected.temporality.as_deref(),
        errors,
    );

    let codegen_value_type = definition
        .annotations
        .code_generation
        .as_ref()
        .map(|annotation| annotation.metric_value_type.as_str());
    let derived_value_type = match codegen_value_type {
        Some("int") => "u64",
        Some("double") => "f64",
        _ => "",
    };
    check_equal(
        &format!("metric {metric} derived Rust value type"),
        derived_value_type,
        expected.value_type.as_str(),
        errors,
    );

    let effective_availability = annotation
        .availability
        .as_ref()
        .or(metric_set.availability.as_ref());
    check_equal(
        &format!("metric {metric} effective availability"),
        &effective_availability,
        &expected.availability.as_ref(),
        errors,
    );
    if annotation.availability.is_some() && metric_set.availability.is_some() {
        errors.push(format!(
            "metric {metric} redundantly overrides metric-set availability"
        ));
    }
}

fn compare_keys<'a>(
    kind: &str,
    expected: impl Iterator<Item = &'a String>,
    actual: impl Iterator<Item = &'a String>,
    errors: &mut Vec<String>,
) {
    let expected = expected.cloned().collect::<BTreeSet<_>>();
    let actual = actual.cloned().collect::<BTreeSet<_>>();
    for missing in expected.difference(&actual) {
        errors.push(format!("missing {kind} definition {missing}"));
    }
    for stale in actual.difference(&expected) {
        errors.push(format!("stale {kind} definition {stale}"));
    }
}

fn check_equal<T>(label: &str, actual: &T, expected: &T, errors: &mut Vec<String>)
where
    T: std::fmt::Debug + PartialEq + ?Sized,
{
    if actual != expected {
        errors.push(format!(
            "{label}: registry has {actual:?}, source requires {expected:?}"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    /// Scenario: an attribute field uses an enum deriving AttributeEnum with a custom wire value.
    /// Guarantees: the inventory preserves every member value, member description, and development stability.
    #[test]
    fn derived_attribute_enum_is_inferred() {
        let item: ItemEnum = syn::parse_quote! {
            #[derive(Debug, AttributeEnum)]
            enum Response {
                /// Successful response.
                #[attribute_value = "http_2xx"]
                Http2xx,
                /// Transport failed.
                NetworkError,
            }
        };
        let mut inventory = Inventory::default();
        inventory
            .scan_attribute_enum(&item, "otap-df-telemetry")
            .unwrap();

        let AttributeType::Enum { members } =
            inventory.resolve_attribute_type("otap-df-telemetry", &syn::parse_quote!(Response))
        else {
            panic!("Response must resolve as an enum attribute");
        };
        assert_eq!(
            members,
            vec![
                AttributeEnumMember {
                    id: "http_2xx".to_owned(),
                    value: "http_2xx".to_owned(),
                    brief: Some("Successful response.".to_owned()),
                    stability: "development".to_owned(),
                },
                AttributeEnumMember {
                    id: "network_error".to_owned(),
                    value: "network_error".to_owned(),
                    brief: Some("Transport failed.".to_owned()),
                    stability: "development".to_owned(),
                },
            ]
        );
    }

    /// Scenario: a foreign enum implements AttributeEnum manually with a literal VARIANTS list.
    /// Guarantees: the inventory recognizes the implementation as the string enum used by local attribute sets.
    #[test]
    fn manual_attribute_enum_implementation_is_inferred() {
        let item: ItemImpl = syn::parse_quote! {
            impl AttributeEnum for SignalType {
                const CARDINALITY: usize = 3;
                const VARIANTS: &'static [&'static str] = &["traces", "metrics", "logs"];

                fn variant_index(self) -> usize {
                    0
                }
            }
        };
        let mut inventory = Inventory::default();
        inventory
            .scan_attribute_enum_impl(&item, "otap-df-telemetry")
            .unwrap();

        let AttributeType::Enum { members } =
            inventory.resolve_attribute_type("otap-df-telemetry", &syn::parse_quote!(SignalType))
        else {
            panic!("SignalType must resolve as an enum attribute");
        };
        assert_eq!(
            members
                .iter()
                .map(|member| member.value.as_str())
                .collect::<Vec<_>>(),
            ["traces", "metrics", "logs"]
        );
    }

    /// Scenario: an enum-backed declaration shares a key with unknown and plain string call sites.
    /// Guarantees: compatible wire observations retain and union the enum while true primitive conflicts degrade to any.
    #[test]
    fn attribute_type_merge_preserves_compatible_enum_information() {
        let member = |value: &str| AttributeEnumMember {
            id: value.to_owned(),
            value: value.to_owned(),
            brief: None,
            stability: "development".to_owned(),
        };
        let mut r#type = AttributeType::Enum {
            members: vec![member("success")],
        };
        r#type.merge_observation(&AttributeType::any());
        r#type.merge_observation(&AttributeType::Primitive("string".to_owned()));
        r#type.merge(&AttributeType::Enum {
            members: vec![member("failure")],
        });
        assert_eq!(
            r#type,
            AttributeType::Enum {
                members: vec![member("failure"), member("success")],
            }
        );

        r#type.merge(&AttributeType::Primitive("int".to_owned()));
        assert_eq!(r#type, AttributeType::any());
    }

    /// Scenario: registry attributes and events omit redundant wire identifier annotations.
    /// Guarantees: standard identifiers become wire defaults while explicit compatibility overrides still win.
    #[test]
    fn registry_wire_identifiers_use_standard_defaults_and_explicit_overrides() {
        let mut attribute = RegistryAttribute {
            key: "core.id".to_owned(),
            r#type: AttributeType::Primitive("int".to_owned()),
            brief: "Core identifier.".to_owned(),
            stability: "development".to_owned(),
            annotations: RegistryAnnotations::default(),
        };
        assert_eq!(attribute.wire_key(), "core.id");
        attribute.annotations.otap_dataflow.wire = Some(WireAnnotation {
            attribute_key: Some("legacy_core_id".to_owned()),
            ..WireAnnotation::default()
        });
        assert_eq!(attribute.wire_key(), "legacy_core_id");

        let mut event = RegistryEvent {
            name: "pipeline.started".to_owned(),
            brief: "Pipeline started.".to_owned(),
            stability: "development".to_owned(),
            requirement_level: "recommended".to_owned(),
            attributes: Vec::new(),
            entity_associations: Vec::new(),
            annotations: RegistryAnnotations::default(),
        };
        assert_eq!(event.wire_name(), "pipeline.started");
        event.annotations.otap_dataflow.wire = Some(WireAnnotation {
            event_name: Some("Legacy.PipelineStarted".to_owned()),
            ..WireAnnotation::default()
        });
        assert_eq!(event.wire_name(), "Legacy.PipelineStarted");
    }

    /// Scenario: a generated SDK package opts out of the production source inventory through Cargo metadata.
    /// Guarantees: generated consumers can be excluded while ordinary workspace packages remain included by default.
    #[test]
    fn package_metadata_controls_source_inventory() {
        let generated = serde_json::json!({
            "metadata": {"semconv": {"source_inventory": false}}
        });
        let production = serde_json::json!({"metadata": {}});

        assert!(!package_is_source_inventory(&generated));
        assert!(package_is_source_inventory(&production));
    }

    /// Scenario: cfg predicates combine test-only and production feature branches.
    /// Guarantees: definitely test-only code is excluded while a possible production branch remains discoverable.
    #[test]
    fn production_cfg_evaluation_is_conservative() {
        let test: Meta = syn::parse_quote!(test);
        let test_or_unix: Meta = syn::parse_quote!(any(test, unix));
        let test_and_unix: Meta = syn::parse_quote!(all(test, unix));
        assert_eq!(evaluate_production_cfg(&test).unwrap(), Truth::False);
        assert_eq!(
            evaluate_production_cfg(&test_or_unix).unwrap(),
            Truth::Unknown
        );
        assert_eq!(
            evaluate_production_cfg(&test_and_unix).unwrap(),
            Truth::False
        );
    }

    /// Scenario: a metric field uses the default dotted name and a cumulative observed counter.
    /// Guarantees: the source inventory preserves the generated name, semantic instrument, Rust instrument, value type, and temporality.
    #[test]
    fn metric_shape_matches_macro_contract() {
        let shape = metric_shape(&syn::parse_quote!(ObserveCounter<u64>)).unwrap();
        assert_eq!(shape.instrument, "counter");
        assert_eq!(shape.rust_instrument, "ObserveCounter");
        assert_eq!(shape.value_type, "u64");
        assert_eq!(shape.temporality.as_deref(), Some("cumulative"));
    }

    /// Scenario: a metric field uses one of the exponential-histogram distribution tiers.
    /// Guarantees: both tiers retain their concrete Rust type while sharing the standard histogram shape.
    #[test]
    fn metric_shape_preserves_distribution_histogram_tier() {
        for rust_instrument in ["HistogramNormal", "HistogramDetailed"] {
            let ty: Type = syn::parse_str(rust_instrument).unwrap();
            let shape = metric_shape(&ty).unwrap();
            assert_eq!(shape.instrument, "histogram");
            assert_eq!(shape.rust_instrument, rust_instrument);
            assert_eq!(shape.value_type, "f64");
            assert_eq!(shape.temporality.as_deref(), Some("delta"));
        }
    }

    /// Scenario: standard metric instruments use project defaults with sparse recording-mode overrides.
    /// Guarantees: code generation selects the current Rust instrument and temporality without duplicating them per metric.
    #[test]
    fn metric_generation_mapping_uses_defaults_and_sparse_overrides() {
        assert_eq!(
            generated_metric_shape("counter", None),
            Some(("Counter", Some("delta")))
        );
        assert_eq!(
            generated_metric_shape("counter", Some("observed")),
            Some(("ObserveCounter", Some("cumulative")))
        );
        assert_eq!(
            generated_metric_shape("updowncounter", None),
            Some(("ObserveUpDownCounter", Some("cumulative")))
        );
        assert_eq!(
            generated_metric_shape("updowncounter", Some("additive")),
            Some(("UpDownCounter", Some("cumulative")))
        );
        assert_eq!(
            generated_metric_shape("histogram", None),
            Some(("Mmsc", Some("delta")))
        );
        assert_eq!(generated_metric_shape("gauge", Some("observed")), None);
    }

    /// Scenario: a module declares a component telemetry scope alongside ordinary items.
    /// Guarantees: the source inventory extracts the component target that unqualified event macros inherit.
    #[test]
    fn component_scope_target_is_discovered_from_module_items() {
        let syntax = syn::parse_file(
            r#"
            otap_df_telemetry::otel_component_scope!(
                urn = COMPONENT_URN,
                target = "otel.processor.transform",
            );
            fn process() {}
            "#,
        )
        .unwrap();

        assert_eq!(
            component_scope_target(&syntax.items).unwrap().as_deref(),
            Some("otel.processor.transform")
        );
    }

    /// Scenario: component events appear inside an executable `tokio::select!` body and use both local and qualified macros.
    /// Guarantees: nested local calls inherit the component target while qualified calls retain the package target.
    #[test]
    fn event_inventory_discovers_nested_calls_with_their_effective_scopes() {
        let expression: ExprMacro = syn::parse_quote! {
            tokio::select! {
                value = receive() => otel_debug!("component.receive", value = ?value),
                _ = shutdown() => otap_df_telemetry::otel_warn!("package.shutdown"),
            }
        };
        let mut inventory = Inventory::default();
        let mut visitor = EventVisitor {
            inventory: &mut inventory,
            package: "otap-df-core-nodes",
            event_scope: "otel.exporter.otlp_grpc",
            source: "src/exporter.rs",
            availability: None,
            errors: Vec::new(),
        };
        visitor.scan_macro(&expression.mac);
        assert!(visitor.errors.is_empty(), "{:?}", visitor.errors);
        drop(visitor);

        assert_eq!(
            inventory.events["component.receive"].scopes,
            BTreeSet::from(["otel.exporter.otlp_grpc".to_owned()])
        );
        assert_eq!(
            inventory.events["package.shutdown"].scopes,
            BTreeSet::from(["otap-df-core-nodes".to_owned()])
        );
    }

    /// Scenario: an event macro contains formatted fields, primitive literals, and a log body.
    /// Guarantees: only structured attributes are inventoried and display/debug values use string wire types.
    #[test]
    fn event_parser_separates_attributes_from_body() {
        let parsed = parse_event_macro(
            "otel_warn",
            quote!("export.failed", error = %err, retries = 3, "request failed: {}", err),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(parsed.name, "export.failed");
        assert_eq!(parsed.severity, "warn");
        assert_eq!(
            parsed.attributes.get("error").map(String::as_str),
            Some("string")
        );
        assert_eq!(
            parsed.attributes.get("retries").map(String::as_str),
            Some("int")
        );
        assert_eq!(parsed.attributes.len(), 2);
    }

    /// Scenario: an event name is supplied through a unique string constant.
    /// Guarantees: constant-backed macro callsites resolve to the same stable event name as literal callsites.
    #[test]
    fn event_parser_resolves_string_constants() {
        let constants = BTreeMap::from([(
            "EXPORT_FAILED".to_owned(),
            BTreeSet::from(["export.failed".to_owned()]),
        )]);
        let parsed = parse_event_macro(
            "otel_error",
            quote!(telemetry::EXPORT_FAILED, reason = "timeout"),
            &constants,
        )
        .unwrap();
        assert_eq!(parsed.name, "export.failed");
    }

    /// Scenario: an emitted event name is either valid v2 syntax or contains uppercase segments.
    /// Guarantees: valid wire names remain stable while invalid names receive a deterministic `otap.*` alias.
    #[test]
    fn canonical_event_names_preserve_valid_names_and_alias_invalid_names() {
        assert_eq!(canonical_event_name("pipeline.start"), "pipeline.start");
        assert_eq!(
            canonical_event_name("Socket.KeepaliveRetriesIgnored"),
            "otap.socket.keepalive_retries_ignored"
        );
    }
}
