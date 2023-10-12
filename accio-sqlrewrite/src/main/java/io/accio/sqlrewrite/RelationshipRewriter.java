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

package io.accio.sqlrewrite;

import io.accio.sqlrewrite.analyzer.ExpressionRelationshipInfo;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class RelationshipRewriter
        extends BaseRewriter<Void>
{
    //    private static final Logger LOG = Logger.get(ExpressionRelationshipAnalyzer.class);
    private final Map<QualifiedName, DereferenceExpression> replacements;

    public static Node rewrite(List<ExpressionRelationshipInfo> relationshipInfos, Expression expression)
    {
        requireNonNull(relationshipInfos);
        return new RelationshipRewriter(
                relationshipInfos.stream()
                        .collect(toUnmodifiableMap(ExpressionRelationshipInfo::getQualifiedName, RelationshipRewriter::toDereferenceExpression)))
                .process(expression);
    }

    public RelationshipRewriter(Map<QualifiedName, DereferenceExpression> replacements)
    {
        this.replacements = requireNonNull(replacements);
    }

    @Override
    protected Node visitDereferenceExpression(DereferenceExpression node, Void ignored)
    {
        if (node.getField().isPresent()) {
            QualifiedName qualifiedName = getQualifiedName(node);
            if (qualifiedName != null) {
//                LOG.debug("replace %s with %s, expressionRelationshipInfo: %s", node.toString(), newNode.toString(), expressionRelationshipInfo.toString());
                return replacements.get(qualifiedName) == null ? node : replacements.get(qualifiedName);
            }
        }
        return node;
    }

    private static DereferenceExpression toDereferenceExpression(ExpressionRelationshipInfo expressionRelationshipInfo)
    {
        String base = expressionRelationshipInfo.getRelationships().get(expressionRelationshipInfo.getRelationships().size() - 1).getModels().get(1);
        List<String> parts = new ArrayList<>();
        parts.add(base);
        parts.addAll(expressionRelationshipInfo.getRemainingParts());
        return (DereferenceExpression) DereferenceExpression.from(QualifiedName.of(parts));
    }
}