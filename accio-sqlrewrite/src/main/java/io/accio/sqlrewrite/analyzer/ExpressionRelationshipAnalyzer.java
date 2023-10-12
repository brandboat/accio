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

import io.accio.base.AccioMDL;
import io.accio.base.dto.Column;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.accio.base.AccioMDL.getRelationshipColumn;
import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExpressionRelationshipAnalyzer
{
    public static List<ExpressionRelationshipInfo> getRelationships(Expression expression, AccioMDL mdl, Model model)
    {
        RelationshipCollector collector = new RelationshipCollector(mdl, model);
        collector.process(expression);
        return collector.getExpressionRelationshipInfo();
    }

    private static class RelationshipCollector
            extends DefaultTraversalVisitor<Void>
    {
        private final AccioMDL accioMDL;
        private final Model model;
        private final List<ExpressionRelationshipInfo> relationships = new ArrayList<>();

        public RelationshipCollector(AccioMDL accioMDL, Model model)
        {
            this.accioMDL = requireNonNull(accioMDL);
            this.model = requireNonNull(model);
        }

        public List<ExpressionRelationshipInfo> getExpressionRelationshipInfo()
        {
            return relationships;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            if (node.getField().isPresent()) {
                QualifiedName qualifiedName = getQualifiedName(node);
                if (qualifiedName != null) {
                    ExpressionRelationshipInfo expressionRelationshipInfo = createRelationshipInfo(qualifiedName, model, accioMDL);
                    if (expressionRelationshipInfo.isNeedToReplace()) {
                        validateToOne(expressionRelationshipInfo);
                        relationships.add(expressionRelationshipInfo);
                    }
                }
            }
            return null;
        }
    }

    private static ExpressionRelationshipInfo createRelationshipInfo(QualifiedName qualifiedName, Model model, AccioMDL mdl)
    {
        List<Relationship> relationships = new ArrayList<>();
        Model current = model;
        for (int i = 0; i < qualifiedName.getParts().size(); i++) {
            String columnName = qualifiedName.getParts().get(i);
            Optional<Column> relationshipColumn = getRelationshipColumn(current, columnName);
            if (relationshipColumn.isPresent()) {
                Column column = relationshipColumn.get();
                String relationshipName = column.getRelationship().get();
                Relationship relationship = mdl.getRelationship(relationshipName)
                        .orElseThrow(() -> new NoSuchElementException(format("relationship %s not found", relationshipName)));
                relationships.add(reverseIfNeeded(relationship, column.getType()));
            }
            else {
                return new ExpressionRelationshipInfo(
                        qualifiedName,
                        qualifiedName.getParts().subList(0, i),
                        qualifiedName.getParts().subList(i, qualifiedName.getParts().size()),
                        relationships);
            }

            // relationship column type is model name
            current = mdl.getModel(relationshipColumn.get().getType())
                    .orElseThrow(() -> new NoSuchElementException(format("model %s not found", relationshipColumn.get().getType())));
        }
        return new ExpressionRelationshipInfo(qualifiedName, qualifiedName.getParts(), List.of(), relationships);
    }

    private static Relationship reverseIfNeeded(Relationship relationship, String firstModelName)
    {
        if (relationship.getModels().get(1).equals(firstModelName)) {
            return relationship;
        }
        return Relationship.reverse(relationship);
    }

    private static void validateToOne(ExpressionRelationshipInfo expressionRelationshipInfo)
    {
        for (Relationship relationship : expressionRelationshipInfo.getRelationships()) {
            checkArgument(relationship.getJoinType().isToOne(), "expr in model only accept to-one relation");
        }
    }
}
