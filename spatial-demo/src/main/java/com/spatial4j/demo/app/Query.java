package com.spatial4j.demo.app;

import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;

import java.io.Serializable;

public class Query implements Serializable
{
  public String fq;
  public String source ="(all)";

  public String field = "geohash";
  public SpatialOperation op = SpatialOperation.Intersects;
  public String geo;

  public Boolean score;
  public Double distErrPct;
  public String sort;

  public SolrParams toSolrQuery( int rows )
  {
    SolrQuery params = new SolrQuery();
    String q = "";

    boolean hasGeo = (geo != null && geo.length() > 0);
    if( hasGeo ) {
      if (Boolean.TRUE.equals(score)) {
        q += "{! score=distance}";
      }
      q += field + ":\""+op.toString()+"("+geo+")";
      if( distErrPct != null ) {
        q += " distErrPct=" + distErrPct;
      }
      q += '"';
    }
    else {
      q = "*:*";
    }
    if( source != null && !(source.startsWith( "(" )) ) {
      params.addFilterQuery( "source:"+source );
    }
    if( fq != null ) {
      params.addFilterQuery( fq );
    }

    // Set sort
    if( sort != null ) {
      params.set( CommonParams.SORT, sort );
    }

    params.setQuery( q );
    params.setRows( rows );
    params.setFields( "id,name,source,score" );
    return params;
  }
}
