import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import App from './App.jsx';

import { Amplify } from 'aws-amplify';
import config from './authConfig';
Amplify.configure(config);

import { Authenticator } from '@aws-amplify/ui-react';
import '@aws-amplify/ui-react/styles.css';

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <Authenticator>
      {({ signOut, user }) => <App />}
    </Authenticator>
  </StrictMode>
);

