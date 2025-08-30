import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App.jsx';
import './index.css'; // אם יש לך עיצוב כללי

import { Amplify } from 'aws-amplify'; // ✅
import awsconfig from './aws-exports'; // 👈 זה נוצר ע"י Amplify CLI

// הגדרת חיבור ל-AWS Amplify (Cognito + S3)
Amplify.configure(awsconfig);

// יצירת root והצגת האפליקציה
ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
