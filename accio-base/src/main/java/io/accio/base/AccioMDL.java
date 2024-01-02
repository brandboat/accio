/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.accio.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.hubspot.jinjava.Jinjava;
import io.accio.base.dto.CacheInfo;
import io.accio.base.dto.Column;
import io.accio.base.dto.CumulativeMetric;
import io.accio.base.dto.DateSpine;
import io.accio.base.dto.EnumDefinition;
import io.accio.base.dto.Manifest;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import io.accio.base.dto.View;
import io.accio.base.jinjava.JinjavaExpressionProcessor;
import io.accio.base.jinjava.JinjavaUtils;
import io.trino.sql.tree.QualifiedName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.accio.base.macro.Parameter.TYPE.MACRO;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class AccioMDL
{
    public static final AccioMDL EMPTY = AccioMDL.fromManifest(Manifest.builder().setCatalog("").setSchema("").build());
    private static final ObjectMapper MAPPER = new ObjectMapper().disable(FAIL_ON_UNKNOWN_PROPERTIES);
    private static final Jinjava JINJAVA = new Jinjava();

    private final String catalog;
    private final String schema;
    private final Manifest manifest;
    private final Map<String, Model> models;
    private final Map<String, Metric> metrics;
    private final Map<QualifiedName, Column> modelColumns;
    private final Map<QualifiedName, Column> metricColumns;

    public static AccioMDL fromJson(String manifest)
            throws JsonProcessingException
    {
        return new AccioMDL(MAPPER.readValue(manifest, Manifest.class));
    }

    public static AccioMDL fromManifest(Manifest manifest)
    {
        return new AccioMDL(manifest);
    }

    private AccioMDL(Manifest manifest)
    {
        requireNonNull(manifest, "manifest is null");
        this.manifest = renderManifest(manifest);
        this.catalog = manifest.getCatalog();
        this.schema = manifest.getSchema();
        this.models = listModels().stream().collect(toImmutableMap(Model::getName, identity()));
        this.metrics = listMetrics().stream().collect(toImmutableMap(Metric::getName, identity()));
        this.modelColumns = new HashMap<>();
        for (Model model : listModels()) {
            for (Column column : model.getColumns()) {
                modelColumns.put(QualifiedName.of(model.getName(), column.getName()), column);
            }
        }
        this.metricColumns = new HashMap<>();
        for (Metric metric : listMetrics()) {
            for (Column column : metric.getColumns()) {
                metricColumns.put(QualifiedName.of(metric.getName(), column.getName()), column);
            }
        }
    }

    private Manifest renderManifest(Manifest original)
    {
        String macroTags = original.getMacros().stream()
                .filter(macro -> macro.getParameters().stream().noneMatch(parameter -> parameter.getType() == MACRO))
                .map(JinjavaUtils::getMacroTag).collect(joining("\n"));
        List<Model> renderedModels = original.getModels().stream().map(model -> {
            List<Column> processed = model.getColumns().stream().map(column -> renderExpression(column, macroTags, original)).collect(toList());
            return new Model(
                    model.getName(),
                    model.getRefSql(),
                    model.getBaseObject(),
                    processed,
                    model.getPrimaryKey(),
                    model.isCached(),
                    model.getRefreshTime(),
                    model.getDescription());
        }).collect(toList());

        List<Metric> renderedMetrics = original.getMetrics().stream().map(metric ->
                new Metric(metric.getName(),
                        metric.getBaseObject(),
                        metric.getDimension().stream().map(column -> renderExpression(column, macroTags, original)).collect(toList()),
                        metric.getMeasure().stream().map(column -> renderExpression(column, macroTags, original)).collect(toList()),
                        metric.getTimeGrain(),
                        metric.isCached(), metric.getRefreshTime(), metric.getDescription())
        ).collect(toList());

        return Manifest.builder(original)
                .setModels(renderedModels)
                .setMetrics(renderedMetrics)
                .build();
    }

    private Column renderExpression(Column original, String macroTags, Manifest unProcessedManifest)
    {
        if (original.getExpression().isEmpty()) {
            return original;
        }

        String withTag = macroTags + JinjavaExpressionProcessor.process(original.getExpression().get(), unProcessedManifest.getMacros());
        String expression = JINJAVA.render(withTag, ImmutableMap.of());
        return new Column(original.getName(),
                original.getType(),
                original.getRelationship().orElse(null),
                original.isCalculated(),
                original.isNotNull(),
                expression,
                original.getDescription());
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public List<Model> listModels()
    {
        return manifest.getModels();
    }

    public Optional<Model> getModel(String name)
    {
        return Optional.ofNullable(models.get(name));
    }

    public List<Relationship> listRelationships()
    {
        return manifest.getRelationships();
    }

    public Optional<Relationship> getRelationship(String name)
    {
        return manifest.getRelationships().stream()
                .filter(relationship -> relationship.getName().equals(name))
                .findAny();
    }

    public List<EnumDefinition> listEnums()
    {
        return manifest.getEnumDefinitions();
    }

    public Optional<EnumDefinition> getEnum(String name)
    {
        return manifest.getEnumDefinitions().stream()
                .filter(enumField -> enumField.getName().equals(name))
                .findAny();
    }

    public List<Metric> listMetrics()
    {
        return manifest.getMetrics();
    }

    public List<CacheInfo> listCached()
    {
        return Stream.concat(manifest.getMetrics().stream(), manifest.getModels().stream())
                .filter(CacheInfo::isCached)
                .collect(toImmutableList());
    }

    public Optional<CacheInfo> getCacheInfo(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return listCached().stream()
                    .filter(cacheInfo -> cacheInfo.getName().equals(name.getSchemaTableName().getTableName()))
                    .findAny();
        }
        return Optional.empty();
    }

    public Optional<Metric> getMetric(String name)
    {
        return Optional.ofNullable(metrics.get(name));
    }

    public Optional<Metric> getMetric(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return getMetric(name.getSchemaTableName().getTableName());
        }
        return Optional.empty();
    }

    public Optional<CumulativeMetric> getCumulativeMetric(String name)
    {
        return manifest.getCumulativeMetrics().stream()
                .filter(metric -> metric.getName().equals(name))
                .findAny();
    }

    public Optional<CumulativeMetric> getCumulativeMetric(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return getCumulativeMetric(name.getSchemaTableName().getTableName());
        }
        return Optional.empty();
    }

    public Optional<View> getView(String name)
    {
        return manifest.getViews().stream()
                .filter(view -> view.getName().equals(name))
                .findAny();
    }

    public Optional<View> getView(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return getView(name.getSchemaTableName().getTableName());
        }
        return Optional.empty();
    }

    public static Optional<Column> getRelationshipColumn(Model model, String name)
    {
        return getColumn(model, name)
                .filter(column -> column.getRelationship().isPresent());
    }

    private static Optional<Column> getColumn(Model model, String name)
    {
        requireNonNull(model);
        requireNonNull(name);
        return model.getColumns().stream()
                .filter(column -> column.getName().equals(name))
                .findAny();
    }

    public DateSpine getDateSpine()
    {
        return manifest.getDateSpine();
    }

    // TODO: handle other data objects (metric, cumulative metric, view)
    public List<String> getColumnNames(String name)
    {
        Optional<Model> model = getModel(name);
        if (model.isPresent()) {
            return model.get().getColumns().stream().map(Column::getName).collect(toImmutableList());
        }
        return ImmutableList.of();
    }

    public Optional<Column> getColumn(QualifiedName qualifiedName)
    {
        return Optional.ofNullable(modelColumns.get(qualifiedName))
                .or(() -> Optional.ofNullable(metricColumns.get(qualifiedName)));
    }
}
