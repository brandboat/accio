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

import io.accio.base.AccioMDL;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipAnalyzer;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipInfo;
import io.trino.sql.tree.Expression;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.sqlrewrite.Utils.parseExpression;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionRelationshipRewriter
{
    private AccioMDL mdl;
    private Model orders;
    private Relationship ordersCustomer;
    private Relationship customerNation;

    @BeforeClass
    public void init()
            throws IOException
    {
        mdl = AccioMDL.fromJson(Files.readString(Path.of(getClass().getClassLoader().getResource("tpch_mdl.json").getPath())));
        orders = mdl.getModel("Orders").orElseThrow();
        ordersCustomer = mdl.getRelationship("OrdersCustomer").orElseThrow();
        customerNation = mdl.getRelationship("CustomerNation").orElseThrow();
    }

    @DataProvider
    public Object[][] rewriteTests()
    {
        return new Object[][] {
                {"customer.custkey", "Customer.custkey", List.of(ordersCustomer)},
                {"customer.nation.name", "Nation.name", List.of(ordersCustomer, customerNation)},
                {"customer.nation.nationkey + 1", "(Nation.nationkey + 1)", List.of(ordersCustomer, customerNation)},
                {"concat('n#', customer.nation.name)", "concat('n#', Nation.name)", List.of(ordersCustomer, customerNation)}
        };
    }

    @Test(dataProvider = "rewriteTests")
    public void testRewrite(String actual, String expected, List<Relationship> relationships)
    {
        Expression expression = parseExpression(actual);
        List<ExpressionRelationshipInfo> expressionRelationshipInfos = ExpressionRelationshipAnalyzer.getRelationships(expression, mdl, orders);
        assertThat(expressionRelationshipInfos.stream().map(ExpressionRelationshipInfo::getRelationships).flatMap(List::stream).collect(toImmutableList()))
                .containsExactlyInAnyOrderElementsOf(relationships);
        assertThat(RelationshipRewriter.rewrite(expressionRelationshipInfos, expression).toString()).isEqualTo(expected);
    }
}
