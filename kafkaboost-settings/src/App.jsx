import React, { useState, useEffect } from 'react';
import './App.css';
import logo from './assets/kafkaboost-logo.png';
import rocketIcon from './assets/rocket-icon.png';
import { Amplify } from 'aws-amplify';
import awsconfig from './aws-exports';
import { getCurrentUser } from 'aws-amplify/auth';
import { uploadData, list, getUrl } from 'aws-amplify/storage';
import { withAuthenticator } from '@aws-amplify/ui-react';
import '@aws-amplify/ui-react/styles.css';

Amplify.configure(awsconfig);

function App() {
  const [stage, setStage] = useState('defineMax');
  const [topics, setTopics] = useState([]);
  const [valRules, setValRules] = useState([]);
  const [defaultPriority, setDefaultPriority] = useState('');
  const [maxPriority, setMaxPriority] = useState('');
  const [successMessage, setSuccessMessage] = useState('');
  const [errorMessage, setErrorMessage] = useState('');
  const [editingSettings, setEditingSettings] = useState(false);
  const [history, setHistory] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(false);

  useEffect(() => {
    fetchSettingsVersions();
    loadLatestSettings();
  }, []);

  const validatePriority = (priority) => {
    const num = parseInt(priority);
    return !isNaN(num) && num >= 1 && num <= parseInt(maxPriority);
  };

  const handleMaxPrioritySubmit = () => {
    if (!defaultPriority || !maxPriority || isNaN(parseInt(maxPriority)) || parseInt(maxPriority) < 1 || isNaN(parseInt(defaultPriority))) {
      setErrorMessage('Please enter valid Default and Max Priority values.');
      return;
    }
    if (parseInt(defaultPriority) > parseInt(maxPriority)) {
      setErrorMessage('Default Priority must be less than or equal to Max Priority.');
      return;
    }
    setErrorMessage('');
    setStage('settings');
  };

  const addTopic = () => setTopics([...topics, { name: '', priority: '' }]);
  const addValRule = () => setValRules([...valRules, { val: '', priority: '' }]);

  const updateTopic = (index, field, value) => {
    const updated = [...topics];
    updated[index][field] = value;
    setTopics(updated);
  };

  const updateValRule = (index, field, value) => {
    const updated = [...valRules];
    updated[index][field] = value;
    setValRules(updated);
  };

  const deleteTopic = (index) => setTopics(topics.filter((_, i) => i !== index));
  const deleteValRule = (index) => setValRules(valRules.filter((_, i) => i !== index));

  const saveSettingsToCloud = async (settings) => {
    try {
      const user = await getCurrentUser();
      const userId = user.userId;
      const timestamp = new Date().toISOString();
      const fileName = `users/${userId}/settings-${timestamp}.json`;

      await uploadData({
        key: fileName,
        data: JSON.stringify(settings, null, 2),
        options: { contentType: 'application/json', accessLevel: 'private' }
      }).result;

      return true;
    } catch (error) {
      console.error("❌ Failed to upload settings:", error);
      return false;
    }
  };

  const handleSubmit = async () => {
    const settings = {
      topics: topics.filter(t => t.name && t.priority),
      valRules: valRules.filter(v => v.val && v.priority),
      defaultPriority,
      priorityRange: { min: "1", max: maxPriority }
    };

    const success = await saveSettingsToCloud(settings);
    if (success) {
      setSuccessMessage("✅ Settings saved successfully!");
      setErrorMessage('');
      fetchSettingsVersions();
    } else {
      setErrorMessage("❌ Failed to save settings to cloud.");
      setSuccessMessage('');
    }
  };

  const fetchSettingsVersions = async () => {
    setLoadingHistory(true);
    try {
      const user = await getCurrentUser();
      const userId = user.userId;

      const result = await list({ prefix: `users/${userId}/`, options: { accessLevel: 'private' } });
      const files = result.items
        .filter(file => file.key.endsWith('.json'))
        .sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified));

      setHistory(files);
    } catch (e) {
      console.warn("⚠️ Failed to load history:", e);
    } finally {
      setLoadingHistory(false);
    }
  };

  const loadLatestSettings = async () => {
    try {
      const user = await getCurrentUser();
      const userId = user.userId;

      const result = await list({ prefix: `users/${userId}/`, options: { accessLevel: 'private' } });
      const jsonFiles = result.items.filter(i => i.key.endsWith('.json')).sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified));
      if (!jsonFiles.length) return;

      const url = await getUrl({ key: jsonFiles[0].key, options: { accessLevel: 'private', expiresIn: 300 } });
      const response = await fetch(url.url);
      const loaded = await response.json();

      setTopics(loaded.topics || []);
      setValRules(loaded.valRules || []);
      setDefaultPriority(loaded.defaultPriority || '');
      setMaxPriority((loaded.priorityRange || {}).max || '');
      setStage('settings');
    } catch (err) {
      console.warn("⚠️ No settings found.");
    }
  };

  const loadVersion = async (key) => {
    try {
      const url = await getUrl({ key, options: { accessLevel: 'private', expiresIn: 300 } });
      const response = await fetch(url.url);
      const loaded = await response.json();

      setTopics(loaded.topics || []);
      setValRules(loaded.valRules || []);
      setDefaultPriority(loaded.defaultPriority || '');
      setMaxPriority((loaded.priorityRange || {}).max || '');
    } catch (err) {
      console.error("❌ Failed to load version", err);
    }
  };

  return (
    <div className="page-container">
      <img src={logo} alt="KafkaBoost Logo" className="logo" />
      <div className="form-container">
        {errorMessage && <div className="error-message">{errorMessage}</div>}
        {successMessage && <div className="success-message">{successMessage}</div>}

        <h3>Settings History:</h3>
        {loadingHistory ? <p>Loading...</p> :
          history.length === 0 ? <p>No previous settings found.</p> :
            <ul>
              {history.map((h, i) => (
                <li key={i} onClick={() => loadVersion(h.key)} style={{ cursor: 'pointer' }}>
                  {new Date(h.lastModified).toLocaleString()}
                </li>
              ))}
            </ul>
        }

        {stage === 'defineMax' ? (
          <>
            <h1>Define Priorities</h1>
            <input placeholder="Enter Default Priority" value={defaultPriority} onChange={e => setDefaultPriority(e.target.value)} className="input-style" />
            <input placeholder="Enter Max Priority" value={maxPriority} onChange={e => setMaxPriority(e.target.value)} className="input-style" />
            <button onClick={handleMaxPrioritySubmit} className="button-style">Continue</button>
          </>
        ) : (
          <>
            <h2>Settings</h2>
            <div style={{ marginBottom: '20px' }}>
              {editingSettings ? (
                <>
                  <input value={defaultPriority} onChange={e => setDefaultPriority(e.target.value)} className="input-style" />
                  <input value={maxPriority} onChange={e => setMaxPriority(e.target.value)} className="input-style" />
                  <button className="save-button" onClick={() => setEditingSettings(false)}>Save Settings</button>
                </>
              ) : (
                <>
                  <p><b>Default Priority:</b> {defaultPriority}</p>
                  <p><b>Max Priority:</b> {maxPriority}</p>
                  <button className="edit-button" onClick={() => setEditingSettings(true)}>Edit Settings</button>
                </>
              )}
            </div>

            <div className="table-header">
              <h3>Topics <button onClick={addTopic} className="plus-button">➕</button></h3>
            </div>
            <table className="styled-table">
              <thead><tr><th>Topic Name</th><th>Priority</th><th>Actions</th></tr></thead>
              <tbody>
                {topics.map((topic, index) => (
                  <tr key={index}>
                    <td><input value={topic.name} onChange={(e) => updateTopic(index, 'name', e.target.value)} className="input-style" /></td>
                    <td><input value={topic.priority} onChange={(e) => updateTopic(index, 'priority', e.target.value)} className="input-style" /></td>
                    <td><button className="delete-button" onClick={() => deleteTopic(index)}>Delete</button></td>
                  </tr>
                ))}
              </tbody>
            </table>

            <div className="table-header">
              <h3>Value-Based Rules <button onClick={addValRule} className="plus-button">➕</button></h3>
            </div>
            <table className="styled-table">
              <thead><tr><th>Value</th><th>Priority</th><th>Actions</th></tr></thead>
              <tbody>
                {valRules.map((val, index) => (
                  <tr key={index}>
                    <td><input value={val.val} onChange={(e) => updateValRule(index, 'val', e.target.value)} className="input-style" /></td>
                    <td><input value={val.priority} onChange={(e) => updateValRule(index, 'priority', e.target.value)} className="input-style" /></td>
                    <td><button className="delete-button" onClick={() => deleteValRule(index)}>Delete</button></td>
                  </tr>
                ))}
              </tbody>
            </table>

            <button className="button-style" onClick={handleSubmit}>💾 Save All Settings</button>
          </>
        )}
      </div>
    </div>
  );
}

export default withAuthenticator(App);
