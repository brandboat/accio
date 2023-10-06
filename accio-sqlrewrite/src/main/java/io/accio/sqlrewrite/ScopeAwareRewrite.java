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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.accio.base.AccioMDL;
import io.accio.base.SessionContext;
import io.accio.sqlrewrite.analyzer.Field;
import io.accio.sqlrewrite.analyzer.Scope;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SubscriptExpression;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getLast;
import static io.accio.sqlrewrite.Utils.analyzeFrom;
import static io.accio.sqlrewrite.Utils.getNextPart;
import static io.accio.sqlrewrite.Utils.parseExpression;
import static io.accio.sqlrewrite.Utils.toQualifiedName;
import static io.trino.sql.QueryUtil.identifier;
import static java.lang.String.format;

/**
 * Rewrite the AST to replace all identifiers or dereference expressions
 * without a relation prefix with the relation prefix.
 */
// TODO: Remove this
public class ScopeAwareRewrite
{
    public static final ScopeAwareRewrite SCOPE_AWARE_REWRITE = new ScopeAwareRewrite();

    public Statement rewrite(Node root, AccioMDL accioMDL, SessionContext sessionContext)
    {
        return (Statement) new Rewriter(accioMDL, sessionContext).process(root);
    }

    private static class Rewriter
            extends BaseRewriter<Scope>
    {
        private final AccioMDL accioMDL;
        private final SessionContext sessionContext;

        public Rewriter(AccioMDL accioMDL, SessionContext sessionContext)
        {
            this.accioMDL = accioMDL;
            this.sessionContext = sessionContext;
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Scope context)
        {
            Scope relationScope;
            if (node.getFrom().isPresent()) {
                relationScope = analyzeFrom(accioMDL, sessionContext, node.getFrom().get(), Optional.ofNullable(context));
            }
            else {
                relationScope = context;
            }
            return super.visitQuerySpecification(node, relationScope);
        }

        @Override
        protected Node visitIdentifier(Identifier node, Scope context)
        {
            if (context.getRelationType().isPresent()) {
                List<Field> field = context.getRelationType().get().resolveFields(QualifiedName.of(node.getValue()));
                if (field.size() == 1) {
                    return new DereferenceExpression(identifier(field.get(0).getRelationAlias()
                            .orElse(toQualifiedName(field.get(0).getModelName()))
                            .getSuffix()), identifier(field.get(0).getColumnName()));
                }
                if (field.size() > 1) {
                    throw new IllegalArgumentException("Ambiguous column name: " + node.getValue());
                }
            }
            return node;
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Scope context)
        {
            if (context != null && context.getRelationType().isPresent()) {
                List<String> parts = getPartsQuietly(node);
                for (int i = 0; i < parts.size(); i++) {
                    List<Field> field = context.getRelationType().get().resolveFields(QualifiedName.of(parts.subList(0, i + 1)));
                    if (field.size() == 1) {
                        Field firstMatch = field.get(0);
                        if (i == 3) {
                            // catalog.schema.table.column
                            return removePrefix(node, 2);
                        }
                        if (i == 2) {
                            // schema.table.column
                            return removePrefix(node, 1);
                        }
                        if (i == 1) {
                            // table.column
                            return node;
                        }
                        return addPrefix(node, identifier(field.get(0).getRelationAlias().orElse(toQualifiedName(field.get(0).getModelName())).getSuffix()));
                    }
                    if (field.size() > 1) {
                        throw new IllegalArgumentException("Ambiguous column name: " + DereferenceExpression.getQualifiedName(node));
                    }
                }
            }
            return node;
        }

        private List<String> getPartsQuietly(Expression expression)
        {
            try {
                return getParts(expression);
            }
            catch (IllegalArgumentException ex) {
                return List.of();
            }
        }

        private List<String> getParts(Expression expression)
        {
            if (expression instanceof Identifier) {
                return ImmutableList.of(((Identifier) expression).getValue());
            }
            else if (expression instanceof DereferenceExpression) {
                DereferenceExpression dereferenceExpression = (DereferenceExpression) expression;
                List<String> baseQualifiedName = getParts(dereferenceExpression.getBase());
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                builder.addAll(baseQualifiedName);
                builder.add(dereferenceExpression.getField().orElseThrow().getValue());
                return builder.build();
            }
            else if (expression instanceof SubscriptExpression) {
                SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
                List<String> baseQualifiedName = getParts(subscriptExpression.getBase());
                if (baseQualifiedName != null) {
                    ImmutableList.Builder<String> builder = ImmutableList.builder();
                    builder.addAll(baseQualifiedName.subList(0, baseQualifiedName.size() - 1));
                    builder.add(format("%s[%s]", getLast(baseQualifiedName), subscriptExpression.getIndex().toString()));
                    return builder.build();
                }
            }
            else {
                throw new IllegalArgumentException("Unsupported node ");
            }
            return ImmutableList.of();
        }
    }

    @VisibleForTesting
    public static Expression addPrefix(Expression source, Identifier prefix)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();

        Expression node = source;
        while (node instanceof DereferenceExpression || node instanceof SubscriptExpression) {
            if (node instanceof DereferenceExpression) {
                DereferenceExpression dereferenceExpression = (DereferenceExpression) node;
                builder.add(dereferenceExpression.getField().orElseThrow());
                node = dereferenceExpression.getBase();
            }
            else {
                SubscriptExpression subscriptExpression = (SubscriptExpression) node;
                Identifier base;
                if (subscriptExpression.getBase() instanceof Identifier) {
                    base = (Identifier) subscriptExpression.getBase();
                }
                else {
                    base = ((DereferenceExpression) subscriptExpression.getBase()).getField().orElseThrow();
                }
                builder.add(new SubscriptExpression(base, subscriptExpression.getIndex()));
                node = getNextPart(subscriptExpression);
            }
        }

        if (node instanceof Identifier) {
            builder.add(node);
        }

        return builder.add(prefix).build().reverse().stream().reduce((a, b) -> {
            if (b instanceof SubscriptExpression) {
                SubscriptExpression subscriptExpression = (SubscriptExpression) b;
                return new SubscriptExpression(new DereferenceExpression(a, (Identifier) subscriptExpression.getBase()), ((SubscriptExpression) b).getIndex());
            }
            else if (b instanceof Identifier) {
                return new DereferenceExpression(a, (Identifier) b);
            }
            throw new IllegalArgumentException(format("Unexpected expression: %s", b));
        }).orElseThrow(() -> new IllegalArgumentException(format("Unexpected expression: %s", source)));
    }

    private static Expression removePrefix(DereferenceExpression dereferenceExpression, int removeFirstN)
    {
        return parseExpression(
                Arrays.stream(dereferenceExpression.toString().split("\\."))
                        .skip(removeFirstN)
                        .collect(Collectors.joining(".")));
    }
}
