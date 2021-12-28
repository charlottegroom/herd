import React from "react";
import { withRouter } from "react-router";
import { Layout } from "antd";
import { ApolloProvider as ApolloHooksProvider } from "@apollo/react-hooks";
import { ApolloProvider } from "react-apollo";
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import client from "./graphql/client";

import Header from './components/Header';

const API_URL = 'http://localhost:4000';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NDA1OTI5NDIsImV4cCI6MTY0MDY3OTM0Mn0.tTpV3b6mJ5eFU5CbebUntJIQjSTXrXD1RZg5ORJpD6A';
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`,
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
