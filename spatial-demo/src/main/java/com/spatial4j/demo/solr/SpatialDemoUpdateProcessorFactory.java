package com.spatial4j.demo.solr;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.shape.Shape;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;


public class SpatialDemoUpdateProcessorFactory extends UpdateRequestProcessorFactory
{
  private static final Logger log = LoggerFactory.getLogger(SpatialDemoUpdateProcessorFactory.class);

  private SpatialContext ctx;

  private String sourceFieldName;

  @SuppressWarnings("unchecked")
  @Override
  public void init(NamedList args)
  {
    sourceFieldName = (String) args.get("shapeField");

    final NamedList spatialContextNL = (NamedList<Object>) args.get("SpatialContext");
    Map<String,String> ctxMap = new LinkedHashMap<>();
    for (Map.Entry entry : (Iterable<Map.Entry>)spatialContextNL) {
      ctxMap.put((String) entry.getKey(), entry.getValue().toString());
    }
    ctx = SpatialContextFactory.makeSpatialContext(ctxMap, null);
  }

  @Override
  public DemoUpdateProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next)
  {
    return new DemoUpdateProcessor(next);
  }

  class DemoUpdateProcessor extends UpdateRequestProcessor
  {
    public DemoUpdateProcessor(UpdateRequestProcessor next) {
      super(next);
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException
    {
      // This converts the 'geo' field to a shape
      SolrInputField shapeField = cmd.solrDoc.get( sourceFieldName );
      if( shapeField != null ) {
        if( shapeField.getValueCount() > 1 ) {
          throw new RuntimeException( "multiple values found for 'geometry' field: "+shapeField.getValue() );
        }
        if( !(shapeField.getValue() instanceof Shape) ) {
          Shape shape;
          try {
            shape = ctx.readShapeFromWkt(shapeField.getValue().toString());
          } catch (Exception e) {
            log.error("Couldn't read shape with id "+cmd.getPrintableId(), e);
            return;//skip doc
          }
          float boost = shapeField.getBoost();

          //Work-around that SolrInputField treats Collection as multi-value due to ShapeCollection
          if (shape instanceof Collection)
            shape = new ShapeWrapper(shape);// http://issues.apache.org/jira/browse/SOLR-4329

          // TODO lookup field types that subclass AbstractSpatialFieldType

          if (!(shape instanceof ShapeWrapper)) {
            addField(cmd, "geo", shape, boost);//this field type only accepts JtsGeometry shape
          } else {
            log.info("Didn't add a shape {} to field 'geo' because was a collection.", cmd.getPrintableId());
          }

          addField(cmd, "geohash", shape, boost);
          addField(cmd, "quad", shape, boost);
          addField(cmd, "bbox", shape.getBoundingBox(), boost);
          addField(cmd, "ptvector", shape.getCenter(), boost);
        }
      }
      super.processAdd(cmd);
    }

    private void addField(AddUpdateCommand cmd, String name, Shape shape, float boost) {
      SolrInputField field = new SolrInputField(name);
      field.setValue(shape, boost);
      cmd.solrDoc.put(field.getName(), field);
    }
  }
}
