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
package io.prestosql.server.extension.query.history;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.NodeLocation;

import java.io.IOException;

/**
 * This is a Jackson module to parse a presto class (without jackson annotation) from json.
 */
public class PrestoQueryInfoModule
        extends SimpleModule
{
    public PrestoQueryInfoModule()
    {
        addDeserializer(Identifier.class, new IdentifierDeserializer());
    }

    public static class IdentifierDeserializer
            extends StdDeserializer<Identifier>
    {
        public IdentifierDeserializer()
        {
            super(Identifier.class);
        }

        @Override
        public Identifier deserialize(JsonParser p, DeserializationContext ctxt) throws IOException
        {
            JsonNode identifierNode = p.readValueAsTree();
            if (identifierNode.has("location") && !identifierNode.get("location").isNull()) {
                JsonNode locationNode = identifierNode.get("location");
                NodeLocation location = new NodeLocation(locationNode.get("lineNumber").asInt(),
                        locationNode.get("columnNumber").asInt(1));
                return new Identifier(location, identifierNode.get("value").asText(),
                        identifierNode.get("delimited").asBoolean());
            }
            if (identifierNode.get("delimited") != null) {
                return new Identifier(identifierNode.get("value").asText(), identifierNode.get("delimited").asBoolean());
            }
            return new Identifier(identifierNode.get("value").asText());
        }
    }
}
