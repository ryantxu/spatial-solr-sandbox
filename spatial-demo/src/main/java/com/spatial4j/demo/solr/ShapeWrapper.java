package com.spatial4j.demo.solr;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;

public class ShapeWrapper implements Shape {

  private final Shape delegate;

  public ShapeWrapper(Shape delegate) {
    this.delegate = delegate;
  }

  @Override
  public SpatialRelation relate(Shape shape) {
    return delegate.relate(shape);
  }

  @Override
  public Rectangle getBoundingBox() {
    return delegate.getBoundingBox();
  }

  @Override
  public boolean hasArea() {
    return delegate.hasArea();
  }

  @Override
  public double getArea(SpatialContext spatialContext) {
    return delegate.getArea(spatialContext);
  }

  @Override
  public Point getCenter() {
    return delegate.getCenter();
  }

  @Override
  public Shape getBuffered(double v, SpatialContext spatialContext) {
    return delegate.getBuffered(v, spatialContext);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public String toString() {
    return "WRAPPED("+delegate.toString()+")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ShapeWrapper that = (ShapeWrapper) o;

    if (!delegate.equals(that.delegate)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }
}
