/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.json;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.IOException;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from JSON to Flink Table/SQL internal data structure {@link RowData}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
@Internal
public class JsonRowDataDeserializationSchema extends AbstractJsonDeserializationSchema {
    private static final long serialVersionUID = 1L;

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of Flink SQL internal data
     * structures.
     */
    private final JsonToRowDataConverters.JsonToRowDataConverter runtimeConverter;

    public JsonRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        super(rowType, resultTypeInfo, failOnMissingField, ignoreParseErrors, timestampFormat);
        // 基于传入的 RowType 创建一个 JsonToRowDataConverter 对象，用于负责将 JsonNode 树按照 LogicalType 递归转换为 RowData
        this.runtimeConverter =
                new JsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
                        .createConverter(checkNotNull(rowType));
    }

    // 运行时每条消息调用该方法
    @Override
    public RowData deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            return convertToRowData(deserializeToJsonNode(message));
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                    format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    // ObjectMapper 会创建 JsonParser 然后逐字节扫描，产生 Token 序列，START_OBJECT, FIELD_NAME, VALUE_NUMBER_INT, VALUE_STRING, END_OBJECT 等。
    // 内部调用 _readTreeAndClose(parser) 方法遍历 Tokens，遇到容器 Token（对象/数组）创建对应的 ContainerNode 节点，遇到值 Token 创建对应类型的 ValueNode 节点，
    // 会把子节点挂到当前活跃容器节点的内部集合，完成一个容器（遇到 END_OBJECT/END_ARRAY）则把当前活跃容器节点出栈，恢复到上层容器节点，继续处理后续 Token，
    // 最后得到根 JsonNode（ObjectNode / ArrayNode）。
    public JsonNode deserializeToJsonNode(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    public RowData convertToRowData(JsonNode message) {
        return (RowData) runtimeConverter.convert(message);
    }
}
