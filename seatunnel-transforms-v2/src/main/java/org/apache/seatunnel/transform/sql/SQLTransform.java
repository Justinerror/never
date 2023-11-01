/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SQLTransform extends AbstractCatalogSupportTransform {
    public static final String PLUGIN_NAME = "Sql";

    private String query;

    private EngineType engineType;

    private transient SQLEngine sqlEngine;

    public SQLTransform(
            @NonNull SQLTransformConfig sqlTransformConfig,
            @NonNull ReadonlyConfig config,
            @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.query = sqlTransformConfig.getQuery();
        this.engineType = sqlTransformConfig.getEngineType();

        List<String> sourceTableNames = config.get(CommonOptions.SOURCE_TABLE_NAME);
        if (sourceTableNames != null && !sourceTableNames.isEmpty()) {
            this.inputTableName = sourceTableNames.get(0);
        } else {
            this.inputTableName = catalogTable.getTableId().getTableName();
        }
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        String[] fieldNames = new String[columns.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            fieldNames[i] = column.getName();
            fieldTypes[i] = column.getDataType();
        }
        this.inputRowType = new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void open() {
        sqlEngine = SQLEngineFactory.getSQLEngine(engineType);
        sqlEngine.init(
                inputTableName,
                inputCatalogTable != null ? inputCatalogTable.getTableId().getTableName() : null,
                inputRowType,
                query);
    }

    private void tryOpen() {
        if (sqlEngine == null) {
            open();
        }
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        tryOpen();
        return sqlEngine.transformBySQL(inputRow);
    }

    @Override
    protected TableSchema transformTableSchema() {
        tryOpen();
        List<String> inputColumnsMapping = new ArrayList<>();
        SeaTunnelRowType outRowType = sqlEngine.typeMapping(inputColumnsMapping);
        List<String> outputColumns = Arrays.asList(outRowType.getFieldNames());

        TableSchema.Builder builder = TableSchema.builder();
        if (inputCatalogTable.getTableSchema().getPrimaryKey() != null
                && outputColumns.containsAll(
                        inputCatalogTable.getTableSchema().getPrimaryKey().getColumnNames())) {
            builder = builder.primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey().copy());
        }

        List<ConstraintKey> outputConstraintKeys =
                inputCatalogTable.getTableSchema().getConstraintKeys().stream()
                        .filter(
                                key -> {
                                    List<String> constraintColumnNames =
                                            key.getColumnNames().stream()
                                                    .map(
                                                            ConstraintKey.ConstraintKeyColumn
                                                                    ::getColumnName)
                                                    .collect(Collectors.toList());
                                    return outputColumns.containsAll(constraintColumnNames);
                                })
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        builder = builder.constraintKey(outputConstraintKeys);

        String[] fieldNames = outRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = outRowType.getFieldTypes();
        List<Column> columns = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Column simpleColumn = null;
            String inputColumnName = inputColumnsMapping.get(i);
            if (inputColumnName != null) {
                for (Column inputColumn : inputCatalogTable.getTableSchema().getColumns()) {
                    if (inputColumnName.equals(inputColumn.getName())) {
                        simpleColumn = inputColumn;
                        break;
                    }
                }
            }
            Column column;
            if (simpleColumn != null) {
                column =
                        PhysicalColumn.of(
                                fieldNames[i],
                                fieldTypes[i],
                                simpleColumn.getColumnLength(),
                                simpleColumn.isNullable(),
                                simpleColumn.getDefaultValue(),
                                simpleColumn.getComment());
            } else {
                column = PhysicalColumn.of(fieldNames[i], fieldTypes[i], 0, true, null, null);
            }
            columns.add(column);
        }
        return builder.columns(columns).build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    public void close() {
        sqlEngine.close();
    }
}
