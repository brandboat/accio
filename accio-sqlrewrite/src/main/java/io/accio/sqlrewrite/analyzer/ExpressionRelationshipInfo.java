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

import io.accio.base.dto.Relationship;
import io.trino.sql.tree.QualifiedName;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.accio.base.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

public class ExpressionRelationshipInfo
{
    private final QualifiedName qualifiedName;
    // for debug usage
    private final List<String> relationshipParts;
    private final List<String> remainingParts;
    private final List<Relationship> relationships;

    public ExpressionRelationshipInfo(QualifiedName qualifiedName, List<String> relationshipParts, List<String> remainingParts, List<Relationship> relationships)
    {
        this.qualifiedName = requireNonNull(qualifiedName);
        this.relationshipParts = requireNonNull(relationshipParts);
        this.remainingParts = requireNonNull(remainingParts);
        this.relationships = requireNonNull(relationships);
        checkArgument(relationshipParts.size() + remainingParts.size() == qualifiedName.getParts().size(), "mismatch part size");
    }

    public QualifiedName getQualifiedName()
    {
        return qualifiedName;
    }

    public List<String> getRemainingParts()
    {
        return remainingParts;
    }

    public List<Relationship> getRelationships()
    {
        return relationships;
    }

    public boolean isNeedToReplace()
    {
        return relationships.size() > 0 && remainingParts.size() > 0 && relationships.size() > 0;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("qualifiedName", qualifiedName)
                .add("relationshipParts", relationshipParts)
                .add("remainingParts", remainingParts)
                .add("relationships", relationships)
                .toString();
    }
}