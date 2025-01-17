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

package io.accio.sqlrewrite.analyzer;

import io.trino.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Scope
{
    private final Optional<Scope> parent;
    private final Optional<RelationType> relationType;
    private final boolean isTableScope;
    private final Map<String, WithQuery> namedQueries;

    private Scope(Scope parent, RelationType relationType, boolean isTableScope, Map<String, WithQuery> namedQueries)
    {
        this.parent = Optional.ofNullable(parent);
        this.relationType = Optional.ofNullable(relationType);
        this.isTableScope = isTableScope;
        this.namedQueries = requireNonNull(namedQueries, "namedQueries is null");
    }

    public Optional<Scope> getParent()
    {
        return parent;
    }

    public Optional<RelationType> getRelationType()
    {
        return relationType;
    }

    public boolean isTableScope()
    {
        return isTableScope;
    }

    public Optional<WithQuery> getNamedQuery(String name)
    {
        if (namedQueries.containsKey(name)) {
            return Optional.of(namedQueries.get(name));
        }

        if (parent.isPresent()) {
            return parent.get().getNamedQuery(name);
        }

        return Optional.empty();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<Scope> parent = Optional.empty();
        private RelationType relationType;
        private boolean isTableScope;
        private final Map<String, WithQuery> namedQueries = new HashMap<>();

        public Builder relationType(RelationType relationType)
        {
            this.relationType = relationType;
            return this;
        }

        public Builder parent(Optional<Scope> parent)
        {
            checkArgument(this.parent.isEmpty(), "parent is already set");
            this.parent = requireNonNull(parent, "parent is null");
            return this;
        }

        public Builder isTableScope(boolean isTableScope)
        {
            this.isTableScope = isTableScope;
            return this;
        }

        public Builder namedQuery(String name, WithQuery withQuery)
        {
            checkArgument(!containsNamedQuery(name), "Query '%s' is already added", name);
            namedQueries.put(name, withQuery);
            return this;
        }

        public boolean containsNamedQuery(String name)
        {
            return namedQueries.containsKey(name);
        }

        public Scope build()
        {
            return new Scope(parent.orElse(null), relationType, isTableScope, namedQueries);
        }
    }
}
