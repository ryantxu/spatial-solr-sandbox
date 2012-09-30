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
import java.util.Collections;


public class SpatialDemoUpdateProcessorFactory extends UpdateRequestProcessorFactory
{
  private static final Logger log = LoggerFactory.getLogger(SpatialDemoUpdateProcessorFactory.class);

  private SpatialContext ctx;

  private String sourceFieldName;

  @Override
  public void init(NamedList args)
  {
    sourceFieldName = (String) args.get("shapeField");
  }

  @Override
  public DemoUpdateProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next)
  {
    if (ctx == null) {
      ctx = SpatialContextFactory.makeSpatialContext(Collections.<String, String>emptyMap(), req.getCore().getResourceLoader().getClassLoader());
    }
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
            shape = ctx.readShape( shapeField.getValue().toString() );
          } catch (Exception e) {
            log.error("Couldn't read shape with id "+cmd.getPrintableId(), e);
            return;//skip doc
          }
          float boost = shapeField.getBoost();

          SolrInputField field;

          field = new SolrInputField("geo");
          field.setValue(shape, boost);
          cmd.solrDoc.put(field.getName(), field);

          field = new SolrInputField("geohash");
          field.setValue(shape, boost);
          cmd.solrDoc.put(field.getName(), field);

          field = new SolrInputField("quad");
          field.setValue(shape, boost);
          cmd.solrDoc.put(field.getName(), field);

          field = new SolrInputField("bbox");
          field.setValue(shape.getBoundingBox(), boost);
          cmd.solrDoc.put(field.getName(), field);

          field = new SolrInputField("ptvector");
          field.setValue(shape.getCenter(), boost);
          cmd.solrDoc.put(field.getName(), field);
        }
      }
      super.processAdd(cmd);
    }
  }
}
