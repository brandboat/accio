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
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static io.accio.base.AccioMDL.getRelationshipColumn;
import static io.accio.base.dto.JoinType.reverse;
import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ExpressionAnalyzer
{
    private ExpressionAnalyzer() {}

    private final Set<Relationship> relationships = new HashSet<>();

    public static ExpressionAnalysis analyze(
            Expression expression,
            AccioMDL accioMDL,
            Scope scope)
    {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer();
        return expressionAnalyzer.analyzeExpression(expression, accioMDL, scope);
    }

    private ExpressionAnalysis analyzeExpression(
            Expression expression,
            AccioMDL accioMDL,
            Scope scope)
    {
        new Visitor(accioMDL, scope).process(expression);
        return new ExpressionAnalysis(expression, relationships);
    }

    private class Visitor
            extends DefaultTraversalVisitor<Void>
    {
        private final AccioMDL accioMDL;
        private final Scope scope;
        private final Model model = null;

        public Visitor(AccioMDL accioMDL, Scope scope)
        {
            this.accioMDL = requireNonNull(accioMDL, "accioMDL is null");
            this.scope = requireNonNull(scope, "scope is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
//            LinkedList<Identifier> identifiers = getIdentifiers(node);
//
//            Identifier identifier = popQuietly(identifiers);
//            if (identifier != null && identifier.getValue().equals(model.getName())) {
//                identifier = identifiers.pop();
//            }
//
//            while (identifier != null) {
//                getColumn(model, identifier.getValue())
//                        .flatMap(Column::getRelationship)
//                        .ifPresent(relationshipName ->
//                                relationships.add(
//                                        accioMDL.getRelationship(relationshipName)
//                                                .orElseThrow(() -> new NoSuchElementException(format("relationship %s not found", relationshipName)))));
//                identifier = identifiers.pop();
//            }

            QualifiedName qualifiedName = getQualifiedName(node);
            if (qualifiedName != null) {

            }

            // make sure all relationships are defined and valid in current expression
            return null;
        }
    }

    // collect all identifiers in DereferenceExpression, if any expression in DereferenceExpression
    // is not identifier, return empty list.
    // e.g. expression is a.b.c then return List.of(a, b, c)
    //      expression is a.b[0].c then return List.of()
    private static LinkedList<Identifier> getIdentifiers(DereferenceExpression expression)
    {
        Expression current = expression;
        LinkedList<Identifier> elements = new LinkedList<>();
        while (true) {
            if (current instanceof DereferenceExpression) {
                ((DereferenceExpression) current).getField().ifPresent(elements::addFirst);
                current = ((DereferenceExpression) current).getBase();
            }
            else if (current instanceof Identifier) {
                elements.addFirst((Identifier) current);
                break;
            }
            else {
                elements.clear();
                return new LinkedList<>();
            }
        }
        return elements;
    }

    private static <T> T popQuietly(LinkedList<T> list)
    {
        try {
            return list.pop();
        }
        catch (NoSuchElementException ex) {
            return null;
        }
    }
}
