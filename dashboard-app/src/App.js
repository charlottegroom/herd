import React from "react";
import { withRouter } from "react-router";
import { Layout } from "antd";
import { ApolloProvider as ApolloHooksProvider } from "@apollo/react-hooks";
import { ApolloProvider } from "react-apollo";
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import client from "./graphql/client";

import Header from './components/Header';

const jwt = require('jsonwebtoken');
const CUBE_API_SECRET = '7f6d88f09e638149ba20ca552af5bf9df3e803785796ff211b6afb3420ec85cb3e8554843ec87c3e84cb055c75f15769b6cb0aa61b27dce789b21a6f069f50e7'
const cubejsToken = jwt.sign({}, CUBE_API_SECRET, { expiresIn: '30d' });
const cubejsApi = cubejs(cubejsToken, {
  apiUrl: "https://herd-covid19-dashboard-api.herokuapp.com/cubejs-api/v1",
});

const AppLayout = ({ location, children }) => (
  <Layout style={{ height: "100%" }}>
    <Header location={location} />
    <Layout.Content>{children}</Layout.Content>
  </Layout>
);

const App = withRouter(({ location, children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <ApolloProvider client={client}>
      <ApolloHooksProvider client={client}>
          <AppLayout location={location}>{children}</AppLayout>
      </ApolloHooksProvider>
    </ApolloProvider>
  </CubeProvider>
));

export default App;
