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
package io.prestosql.plugin.oracle;

import io.airlift.configuration.Config;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;

public class OracleConfig
        extends BaseJdbcConfig
{
    private boolean includeSynonyms;
    private int fetchSize = 1000;

    public int getFetchSize()
    {
        return fetchSize;
    }

    @Config("oracle.fetch-size")
    public OracleConfig setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    public boolean isIncludeSynonyms()
    {
        return includeSynonyms;
    }

    @Config("oracle.include-synonyms")
    public OracleConfig setIncludeSynonyms(boolean includeSynonyms)
    {
        this.includeSynonyms = includeSynonyms;
        return this;
    }
}
