#pragma once

#include <cmath>
#include <limits>

#include <Functions/geometryConverters.h>

namespace DB
{

/// Accumulates a bounding box from CartesianPoints or GeometricObjects.
/// Check `found` before using xmin/ymin/xmax/ymax.
struct BboxAccumulator
{
    double xmin = std::numeric_limits<double>::infinity();
    double ymin = std::numeric_limits<double>::infinity();
    double xmax = -std::numeric_limits<double>::infinity();
    double ymax = -std::numeric_limits<double>::infinity();
    bool found = false;

    void add(double x, double y)
    {
        if (!std::isfinite(x) || !std::isfinite(y)) return;
        xmin = std::min(xmin, x);
        ymin = std::min(ymin, y);
        xmax = std::max(xmax, x);
        ymax = std::max(ymax, y);
        found = true;
    }

    void add(const CartesianPoint & p) { add(p.x(), p.y()); }

    template <typename Container>
    void addAll(const Container & pts) { for (const auto & p : pts) add(p); }
};

}
