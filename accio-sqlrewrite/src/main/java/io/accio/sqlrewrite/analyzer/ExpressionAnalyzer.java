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

import com.google.common.collect.Lists;
import io.accio.base.AccioMDL;
import io.accio.base.dto.Relationship;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

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

        public Visitor(AccioMDL accioMDL, Scope scope)
        {
            this.accioMDL = requireNonNull(accioMDL, "accioMDL is null");
            this.scope = requireNonNull(scope, "scope is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            return null;
        }
    }

    private static LinkedList<Expression> elements(Expression expression)
    {
        Expression current = expression;
        LinkedList<Expression> elements = new LinkedList<>();
        while (true) {
            if (current instanceof Identifier) {
                elements.add(current);
                // in dereference expression, function call or identifier should be the root node
                break;
            }
            else if (current instanceof DereferenceExpression) {
                elements.add(current);
                current = ((DereferenceExpression) current).getBase();
            }
            else {
                // unexpected node in dereference expression, clear everything and return
                elements.clear();
                break;
            }
        }
        return new LinkedList<>(Lists.reverse(elements));
    }
}
