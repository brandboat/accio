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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Window
{
    private final String name;

    private final String refColumn;

    private final TimeUnit timeUnit;

    private final String start;
    private final String end;

    public static Window window(String name, String refColumn, TimeUnit timeUnit, String start, String end)
    {
        return new Window(name, refColumn, timeUnit, start, end);
    }

    @JsonCreator
    public Window(
            @JsonProperty("name") String name,
            @JsonProperty("refColumn") String refColumn,
            @JsonProperty("timeUnit") TimeUnit timeUnit,
            @JsonProperty("start") String start,
            @JsonProperty("end") String end)
    {
        this.name = name;
        this.refColumn = refColumn;
        this.timeUnit = timeUnit;
        this.start = start;
        this.end = end;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getRefColumn()
    {
        return refColumn;
    }

    @JsonProperty
    public TimeUnit getTimeUnit()
    {
        return timeUnit;
    }

    @JsonProperty
    public String getStart()
    {
        return start;
    }

    @JsonProperty
    public String getEnd()
    {
        return end;
    }

    @Override
    public String toString()
    {
        return "Window{" +
                "name='" + name + '\'' +
                ", refColumn='" + refColumn + '\'' +
                ", timeUnit=" + timeUnit +
                ", start='" + start + '\'' +
                ", end='" + end + '\'' +
                '}';
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

        Window window = (Window) o;
        return Objects.equals(name, window.name) &&
                Objects.equals(refColumn, window.refColumn) &&
                timeUnit == window.timeUnit &&
                Objects.equals(start, window.start) &&
                Objects.equals(end, window.end);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, refColumn, timeUnit, start, end);
    }
}
