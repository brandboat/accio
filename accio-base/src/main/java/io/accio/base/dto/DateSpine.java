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

public class DateSpine
{
    public static final DateSpine DEFAULT = new DateSpine(TimeUnit.DAY, "1970-01-01", "2077-12-31");

    private final TimeUnit unit;
    private final String start;
    private final String end;

    @JsonCreator
    public DateSpine(
            @JsonProperty("unit") TimeUnit unit,
            @JsonProperty("start") String start,
            @JsonProperty("end") String end)
    {
        this.unit = unit;
        this.start = start;
        this.end = end;
    }

    @JsonProperty
    public TimeUnit getUnit()
    {
        return unit;
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
        return "DateSpine{" +
                "unit=" + unit +
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
        DateSpine dateSpine = (DateSpine) o;
        return unit == dateSpine.unit &&
                Objects.equals(start, dateSpine.start) &&
                Objects.equals(end, dateSpine.end);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unit, start, end);
    }
}
