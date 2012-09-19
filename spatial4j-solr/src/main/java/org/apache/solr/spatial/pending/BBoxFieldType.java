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

import org.apache.lucene.spatial.bbox.BBoxStrategy;
import org.apache.solr.schema.AbstractSpatialFieldType;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaAware;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BBoxFieldType extends AbstractSpatialFieldType<BBoxStrategy> implements SchemaAware
{
  protected String doubleFieldName = "double";
  protected String booleanFieldName = "boolean";

  protected final int fieldProps = (INDEXED | TOKENIZED | OMIT_NORMS | OMIT_TF_POSITIONS);

  double queryPower = 1.0;
  double targetPower = 1.0f;
  private int precisionStep;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    String v = args.remove( "doubleType" );
    if( v != null ) {
      doubleFieldName = v;
    }

    v = args.remove( "booleanType" );
    if( v != null ) {
      booleanFieldName = v;
    }
  }

  @Override
  protected BBoxStrategy newSpatialStrategy(String s) {
    BBoxStrategy strategy = new BBoxStrategy(ctx, s);
    strategy.setPrecisionStep(precisionStep);
    return strategy;
  }

  @Override
  public void inform(IndexSchema schema)
  {
    FieldType doubleType = schema.getFieldTypeByName( doubleFieldName );
    FieldType booleanType = schema.getFieldTypeByName( booleanFieldName );

    if( doubleType == null ) {
      throw new RuntimeException( "Can not find double: "+doubleFieldName );
    }
    if( booleanType == null ) {
      throw new RuntimeException( "Can not find boolean: "+booleanFieldName );
    }
    if( !(booleanType instanceof BoolField) ) {
      throw new RuntimeException( "must be a booleanField: "+booleanType );
    }
    if( !(doubleType instanceof TrieDoubleField) ) {
      throw new RuntimeException( "double must be TrieDoubleField: "+doubleType );
    }

    TrieField df = (TrieField)doubleType;
    precisionStep = df.getPrecisionStep();


    List<SchemaField> fields = new ArrayList<SchemaField>( schema.getFields().values() );
    for( SchemaField sf : fields ) {
      if( sf.getType() == this ) {
        String name = sf.getName();
        register( schema, new SchemaField( name + BBoxStrategy.SUFFIX_MINX, doubleType, fieldProps, null ) );
        register( schema, new SchemaField( name + BBoxStrategy.SUFFIX_MAXX, doubleType, fieldProps, null ) );
        register( schema, new SchemaField( name + BBoxStrategy.SUFFIX_MINY, doubleType, fieldProps, null ) );
        register( schema, new SchemaField( name + BBoxStrategy.SUFFIX_MAXY, doubleType, fieldProps, null ) );
        register( schema, new SchemaField( name + BBoxStrategy.SUFFIX_XDL, booleanType, fieldProps, null ) );
      }
    }
  }

  private void register( IndexSchema s, SchemaField sf )
  {
    s.getFields().put( sf.getName(), sf );
  }

}

