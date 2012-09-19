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

import java.io.IOException;
import java.util.Collections;


public class SpatialDemoUpdateProcessorFactory extends UpdateRequestProcessorFactory
{
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
      SolrInputField field = cmd.solrDoc.get( sourceFieldName );
      if( field != null ) {
        if( field.getValueCount() > 1 ) {
          throw new RuntimeException( "multiple values found for 'geometry' field: "+field.getValue() );
        }
        if( !(field.getValue() instanceof Shape) ) {
          Shape shape = ctx.readShape( field.getValue().toString() );
          field.setValue( shape, field.getBoost() );

          SolrInputField bboxField = field.deepCopy();
          bboxField.setValue(shape.getBoundingBox(), field.getBoost());
          cmd.solrDoc.put("bbox", bboxField);

          SolrInputField ptField = field.deepCopy();
          ptField.setValue(shape.getCenter(), field.getBoost());
          cmd.solrDoc.put("vector2d", ptField);
        }
      }
      super.processAdd(cmd);
    }
  }
}
