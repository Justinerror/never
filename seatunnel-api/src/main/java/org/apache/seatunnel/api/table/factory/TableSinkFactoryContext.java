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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.TablePlaceholder;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;

import java.util.Collection;
import java.util.Collections;

@Getter
public class TableSinkFactoryContext extends TableFactoryContext {

    private final CatalogTable catalogTable;

    @VisibleForTesting
    public TableSinkFactoryContext(
            CatalogTable catalogTable, ReadonlyConfig options, ClassLoader classLoader) {
        super(options, classLoader);
        if (catalogTable != null) {
            checkCatalogTableIllegal(Collections.singletonList(catalogTable));
        }
        this.catalogTable = catalogTable;
    }

    public static TableSinkFactoryContext replacePlaceholderAndCreate(
            CatalogTable catalogTable,
            ReadonlyConfig options,
            ClassLoader classLoader,
            Collection<String> excludeTablePlaceholderReplaceKeys) {
        ReadonlyConfig rewriteConfig =
                TablePlaceholder.replaceTablePlaceholder(
                        options, catalogTable, excludeTablePlaceholderReplaceKeys);
        return new TableSinkFactoryContext(catalogTable, rewriteConfig, classLoader);
    }
}
