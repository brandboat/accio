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
package io.accio.cache;

import io.accio.base.CatalogSchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface CachedTableMapping
{
    void putCachedTableMapping(CatalogSchemaTableName catalogSchemaTableName, CacheInfoPair cacheInfoPair);

    CacheInfoPair get(CatalogSchemaTableName cachedTable);

    void remove(CatalogSchemaTableName cachedTable);

    CacheInfoPair getCacheInfoPair(String catalog, String schema, String table);

    Optional<String> convertToCachedTable(CatalogSchemaTableName catalogSchemaTableName);

    Set<Map.Entry<CatalogSchemaTableName, CacheInfoPair>> entrySet();

    List<CacheInfoPair> getCacheInfoPairs(String catalogName, String schemaName);
}
