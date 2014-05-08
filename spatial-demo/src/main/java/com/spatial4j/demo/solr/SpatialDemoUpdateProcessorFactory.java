package com.spatial4j.demo.solr;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.ShapeCollection;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFilter;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
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
    return new DemoUpdateProcessor(next, req.getSchema());
  }

  class DemoUpdateProcessor extends UpdateRequestProcessor
  {
    private final IndexSchema schema;

    public DemoUpdateProcessor(UpdateRequestProcessor next, IndexSchema schema) {
      super(next);
      this.schema = schema;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException
    {
      // This parses the shape string into a shape object, and it copies them to the various
      //  spatial fields we have, making some modifications as needed.
      final SolrInputField shapeField = cmd.solrDoc.get( sourceFieldName );
      if( shapeField != null ) {
        if( shapeField.getValueCount() > 1 ) {
          throw new RuntimeException( "multiple values found for 'geometry' field: "+shapeField.getValue() );
        }
        final String wkt = shapeField.getValue().toString();
        Shape shape;
        try {
          shape = ctx.readShapeFromWkt(wkt);
        } catch (Exception e) {
          log.error("Couldn't parse shape for doc "+cmd.getPrintableId(), e);
          return;//skip doc
        }
        final float boost = shapeField.getBoost();

        //We check existence for the following two fields since they require LSE which is optional
        if (schema.getFieldOrNull("geo") != null) {
          //The "geo" shape only accepts JtsGeometry
          JtsGeometry jtsGeom = toJtsGeom(shape);
          //log.warn("Couldn't index into 'geo' field for doc {}; got class {}", cmd.getPrintableId(), shape.getClass());
          addField(cmd, "geo", jtsGeom, boost);
        }
        if (schema.getFieldOrNull("bbox") != null) {
          addField(cmd, "bbox", shape.getBoundingBox(), boost);
        }

        addField(cmd, "ptvector", shape.getCenter(), boost);

        //Work-around that SolrInputField treats Collection as multi-value due to ShapeCollection
        if (shape instanceof Collection)
          shape = new ShapeWrapper(shape); // http://issues.apache.org/jira/browse/SOLR-4329

        addField(cmd, "geohash", shape, boost);
        addField(cmd, "quad", shape, boost);

      }
      super.processAdd(cmd);
    }

    private JtsGeometry toJtsGeom(Shape shape) {
      //TODO move this to a Spatial4j utility class?
      if (shape instanceof JtsGeometry)
        return (JtsGeometry) shape;
      JtsSpatialContext jtsCtx = (JtsSpatialContext) ctx;
      final Geometry geom;
      if (shape instanceof ShapeCollection) {
        ShapeCollection coll = (ShapeCollection) shape;
        //assume a collection of Polygons... convert to JtsGeometry
        //TODO handle other MULTI shapes?
        final List<Polygon> polygonList = new ArrayList<>(coll.size());
        for (int i = 0; i < coll.size(); i++) {
          final Geometry geomI = jtsCtx.getGeometryFrom(coll.get(i));
          if (geomI instanceof Polygon)
            polygonList.add((Polygon)geomI);
          else if (geomI instanceof MultiPolygon) {//happens when a polygon crosses the dateline; it gets split
            MultiPolygon multiPolygon = (MultiPolygon) geomI;
            multiPolygon.apply(new GeometryFilter() {
              @Override
              public void filter(Geometry geom) {
                if (geom instanceof MultiPolygon)
                  return;//caller will recurse
                polygonList.add((Polygon)geom);
              }
            });
          }
        }
        geom = jtsCtx.getGeometryFactory().createMultiPolygon(polygonList.toArray(new Polygon[polygonList.size()]));
      } else {
        geom = jtsCtx.getGeometryFrom(shape);
      }
      return jtsCtx.makeShape(geom, true, true);
    }

    private void addField(AddUpdateCommand cmd, String name, Object shape, float boost) {
      SolrInputField field = new SolrInputField(name);
      field.setValue(shape, boost);
      cmd.solrDoc.put(field.getName(), field);
    }
  }
}
