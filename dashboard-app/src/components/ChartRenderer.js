import React, { useState } from "react";
import PropTypes from "prop-types";
import { useCubeQuery } from "@cubejs-client/react";
import { Spin, Row, Col, Statistic, Table } from "antd";
import {
  CartesianGrid,
  PieChart,
  Pie,
  Cell,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
  BarChart,
  Bar,
  LineChart,
  Line
} from "recharts";
import styled from 'styled-components';

import "./recharts-theme.less";
import moment from "moment";
import numeral from "numeral";

const numberFormatter = item => numeral(item).format("0.00");
const dateFormatter = item => moment(item).format("DD MMM YY");
const colors = ["#451dfe", "#e52e5c", "#e7d121", "#ca29f6", "#56fe2f", "#2ad3d6", "#fc2011", "#045c10", "#f887d6", "#1379eb"];
const xAxisFormatter = (item) => {
  if (moment(item).isValid()) {
    return dateFormatter(item)
  } else {
    return item;
  }
}

const PieChartResponsive = ({ resultSet, height }) => {
  const [seriesProps, setSeriesProps] = useState(
    resultSet.seriesNames().reduce(
      (a, { key }) => {
        a[key] = false;
        return a;
      },
      { hover: null }
    )
  );

  const handleLegendMouseEnter = (e) => {
    if (!seriesProps[e.payload.category]) {
      setSeriesProps({ ...seriesProps, hover: e.payload.category });
    }
  };

  const handleLegendMouseLeave = (e) => {
    setSeriesProps({ ...seriesProps, hover: null });
  };

  return (
    <ResponsiveContainer width="100%" height={height}>
      <PieChart>
        <Pie
          isAnimationActive={false}
          data={resultSet.chartPivot()}
          nameKey="x"
          dataKey={resultSet.seriesNames()[0].key}
          fill="#8884d8"
        >
          {resultSet.chartPivot().map((series, index) => (
            <Cell
            key={index}
            fill={colors[index % colors.length]}
            fillOpacity={Number(
              seriesProps.hover === series.category || !seriesProps.hover ? 1 : 0.6
              )}
              />
          ))}
        </Pie>
        <Legend
          onMouseOver={handleLegendMouseEnter}
          onMouseOut={handleLegendMouseLeave}
        />
        <Tooltip />
      </PieChart>
    </ResponsiveContainer>
  )
}

const CartesianChart = ({ resultSet, children, ChartComponent, height, chartType }) => {
  const [seriesProps, setSeriesProps] = useState(
    resultSet.seriesNames().reduce(
      (a, { key }) => {
        a[key] = false;
        return a;
      },
      { hover: null }
    )
  );

  const handleLegendMouseEnter = (e) => {
    if (!seriesProps[e.dataKey]) {
      setSeriesProps({ ...seriesProps, hover: e.dataKey });
    }
  };

  const handleLegendMouseLeave = (e) => {
    setSeriesProps({ ...seriesProps, hover: null });
  };

  const selectSeries = (e) => {
    setSeriesProps({
      ...seriesProps,
      [e.dataKey]: !seriesProps[e.dataKey],
      hover: null
    });
  };

  const TypeToChartComponent = {
    line: (
      resultSet.seriesNames().map((series, i) => (
        <Line
          key={series.key}
          stackId="a"
          strokeWidth={3}
          dataKey={series.key}
          name={series.title}
          stroke={colors[i]}
          hide={seriesProps[series.key] === true}
          strokeOpacity={Number(
            seriesProps.hover === series.key || !seriesProps.hover ? 1 : 0.1
          )}
        />
      ))
    ),
    bar: (
      resultSet.seriesNames().map((series, i) => (
        <Bar
          key={series.key}
          stackId="a"
          dataKey={series.key}
          name={series.title}
          fill={colors[i]}
          hide={seriesProps[series.key] === true}
          fillOpacity={Number(
            seriesProps.hover === series.key || !seriesProps.hover ? 1 : 0.6
          )}
        />
      ))
    ),
    area: (
      resultSet.seriesNames().map((series, i) => (
        <Area
          key={series.key}
          stackId="a"
          dataKey={series.key}
          name={series.title}
          stroke={colors[i]}
          fill={colors[i]}
          hide={seriesProps[series.key] === true}
          fillOpacity={Number(
            seriesProps.hover === series.key || !seriesProps.hover ? 1 : 0.6
          )}
        />
      ))
    ),
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
    <ChartComponent margin={{ left: -10 }} data={resultSet.chartPivot()}>
      <XAxis axisLine={false} tickLine={false} tickFormatter={xAxisFormatter} dataKey="x" minTickGap={20} />
      <YAxis axisLine={false} tickLine={false} tickFormatter={numberFormatter} />
      <CartesianGrid vertical={false} />
      { children }
      {TypeToChartComponent[chartType]}
      <Legend
          onClick={selectSeries}
          onMouseOver={handleLegendMouseEnter}
          onMouseOut={handleLegendMouseLeave}
        />
      <Tooltip labelFormatter={dateFormatter} formatter={numberFormatter} />
    </ChartComponent>
  </ResponsiveContainer>
  )
}

const TypeToChartComponent = {
  line: ({ resultSet, height}) => (
    <CartesianChart resultSet={resultSet} height={height} ChartComponent={LineChart} chartType={'line'}>
    </CartesianChart>
  ),
  bar: ({ resultSet, height }) => (
    <CartesianChart resultSet={resultSet} height={height} ChartComponent={BarChart} chartType={'bar'}>
    </CartesianChart>
  ),
  area: ({ resultSet, height }) => (
    <CartesianChart resultSet={resultSet} height={height} ChartComponent={AreaChart} chartType={'area'}>
    </CartesianChart>
  ),
  pie: ({ resultSet, height }) => (
    <PieChartResponsive resultSet={resultSet} height={height}>
    </PieChartResponsive>
  ),
  table: ({ resultSet }) => (
    <Table
      pagination={false}
      columns={resultSet.tableColumns().map(c => ({ ...c, dataIndex: c.key }))}
      dataSource={resultSet.tablePivot()}
    />
  ),
  number: ({ resultSet }) => (
    <Row
      type="flex"
      justify="center"
      align="middle"
      style={{
        height: "100%"
      }}
    >
      <Col>
        {resultSet.seriesNames().map(s => (
          <Statistic value={resultSet.totalRow()[s.key]} />
        ))}
      </Col>
    </Row>
  )
};
const TypeToMemoChartComponent = Object.keys(TypeToChartComponent)
  .map(key => ({
    [key]: React.memo(TypeToChartComponent[key])
  }))
  .reduce((a, b) => ({ ...a, ...b }));

const SpinContainer = styled.div`
  text-align: center;
  padding: auto;
  margin-top: 10px;
`
const Spinner = () => (
  <SpinContainer>
    <Spin size="large"/>
  </SpinContainer>
)

const renderChart = Component => ({ resultSet, error, height }) =>
  (resultSet && <Component height={height} resultSet={resultSet} />) ||
  (error && error.toString()) || <Spinner />;

const ChartRenderer = ({ vizState, chartHeight }) => {
  const { query, chartType } = vizState;
  const component = TypeToMemoChartComponent[chartType];
  const renderProps = useCubeQuery(query);
  return component && renderChart(component)({ height: chartHeight, ...renderProps });
};

ChartRenderer.propTypes = {
  vizState: PropTypes.object,
  cubejsApi: PropTypes.object
};
ChartRenderer.defaultProps = {
  vizState: {},
  chartHeight: 300,
  cubejsApi: null
};
export default ChartRenderer;
