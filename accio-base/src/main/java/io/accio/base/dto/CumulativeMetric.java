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

package io.accio.base.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import java.util.Objects;

public class CumulativeMetric
        implements CacheInfo
{
    public static CumulativeMetric cumulativeMetric(
            String name,
            String baseObject,
            Measure measure,
            Window window)
    {
        return new CumulativeMetric(name, baseObject, measure, window, false, null, null);
    }

    private final String name;
    private final String baseObject;
    private final Measure measure;
    private final Window window;
    private final boolean cached;
    private final Duration refreshTime;
    private final String description;

    @JsonCreator
    public CumulativeMetric(
            @JsonProperty("name") String name,
            @JsonProperty("baseObject") String baseObject,
            @JsonProperty("measure") Measure measure,
            @JsonProperty("window") Window window,
            // preAggregated is deprecated, use cached instead.
            @JsonProperty("cached") @Deprecated @JsonAlias("preAggregated") boolean cached,
            @JsonProperty("refreshTime") Duration refreshTime,
            @JsonProperty("description") String description)
    {
        this.name = name;
        this.baseObject = baseObject;
        this.measure = measure;
        this.window = window;
        this.cached = cached;
        this.refreshTime = refreshTime;
        this.description = description;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getBaseObject()
    {
        return baseObject;
    }

    @JsonProperty
    public Measure getMeasure()
    {
        return measure;
    }

    @JsonProperty
    public Window getWindow()
    {
        return window;
    }

    @JsonProperty
    public boolean isCached()
    {
        return cached;
    }

    @JsonProperty
    public Duration getRefreshTime()
    {
        return refreshTime;
    }

    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, baseObject, measure, window, cached, refreshTime, description);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CumulativeMetric that = (CumulativeMetric) o;
        return cached == that.cached &&
                Objects.equals(name, that.name) &&
                Objects.equals(baseObject, that.baseObject) &&
                Objects.equals(measure, that.measure) &&
                Objects.equals(window, that.window) &&
                Objects.equals(refreshTime, that.refreshTime) &&
                Objects.equals(description, that.description);
    }

    @Override
    public String toString()
    {
        return "CumulativeMetric{" +
                "name='" + name + '\'' +
                ", baseObject='" + baseObject + '\'' +
                ", measure=" + measure +
                ", window=" + window +
                ", cached=" + cached +
                ", refreshTime=" + refreshTime +
                ", description='" + description + '\'' +
                '}';
    }
}
