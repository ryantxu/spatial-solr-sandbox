/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.spatial.pending;

import com.spatial4j.core.context.jts.JtsSpatialContext;
import org.apache.lucene.spatial.pending.jts.JtsGeoStrategy;
import org.apache.solr.schema.AbstractSpatialFieldType;
import org.apache.solr.schema.IndexSchema;

import java.util.Map;


/**
 * This is here because the dependency tree needs work!
 */
public class GeometryFieldType extends AbstractSpatialFieldType<JtsGeoStrategy> {

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    //TODO instead set via args.put(...) before init()
    ctx = JtsSpatialContext.GEO;
  }

  @Override
  protected JtsGeoStrategy newSpatialStrategy(String fieldName) {
    return new JtsGeoStrategy((JtsSpatialContext)ctx, fieldName);
  }
}
