// App.jsx (גרסה מלאה מעודכנת עם Role-Based Rules מלא)
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
  const [priorityBoost, setPriorityBoost] = useState([]);
  const [partitionValue, setPartitionValue] = useState('');
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

  const handleSubmit = async () => {
    if (!defaultPriority || !maxPriority || isNaN(parseInt(maxPriority)) || isNaN(parseInt(defaultPriority))) {
      setErrorMessage("❌ Please provide valid numeric values for Default and Max Priority.");
      return;
    }
    try {
      const user = await getCurrentUser();
      const userId = user.userId;
      const hashCode = user.signInUserSession?.idToken?.jwtToken?.slice(-6) || '000000';

      const settings = {
        user_id: userId,
        hash_code: hashCode,
        max_priority: parseInt(maxPriority),
        default_priority: parseInt(defaultPriority),
        Topics_priority: topics.filter(t => t.name && t.priority).map(t => ({ topic: t.name, priority: parseInt(t.priority) })),
        Rule_Base_priority: valRules.filter(v => v.val && v.priority && v.role_name).map(v => ({ role_name: v.role_name, value: v.val, priority: parseInt(v.priority) })),
        Priority_boost: priorityBoost.map(b => ({ topic_name: b.topic_name, priority_boost_min_value: parseInt(b.priority_boost_min_value),number_of_partitions: parseInt(b.number_of_partitions || '0')  }))
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
    } catch (err) {
      console.error("❌ Failed to prepare settings:", err);
      setErrorMessage("❌ Internal error while preparing settings.");
    }
  };

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

  const fetchSettingsVersions = async () => {
    setLoadingHistory(true);
    try {
      const user = await getCurrentUser();
      const userId = user.userId;

      const result = await list({ prefix: `users/${userId}/`, options: { accessLevel: 'private' } });
      const files = result.items.filter(file => file.key.endsWith('.json')).sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified));

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

      setTopics((loaded.Topics_priority || []).map(t => ({ name: t.topic, priority: t.priority.toString() })));
      setValRules((loaded.Rule_Base_priority || []).map(r => ({ role_name: r.role_name || '', val: r.value, priority: r.priority.toString() })));
      setPriorityBoost((loaded.Priority_boost || []).map(p => ({ topic_name: p.topic_name, priority_boost_min_value: p.priority_boost_min_value.toString() , number_of_partitions: p.number_of_partitions?.toString() || '' })));
      setDefaultPriority(loaded.default_priority?.toString() || '');
      setMaxPriority(loaded.max_priority?.toString() || '');
      setStage('settings');
    } catch (err) {
      console.warn("⚠️ No settings found.", err);
    }
  };

  const loadVersion = async (key) => {
    try {
      const url = await getUrl({ key, options: { accessLevel: 'private', expiresIn: 300 } });
      const response = await fetch(url.url);
      const loaded = await response.json();

      setTopics((loaded.Topics_priority || []).map(t => ({ name: t.topic, priority: t.priority.toString() })));
      setValRules((loaded.Rule_Base_priority || []).map(r => ({ role_name: r.role_name || '', val: r.value, priority: r.priority.toString() })));
      setPriorityBoost((loaded.Priority_boost || []).map(p => ({ topic_name: p.topic_name, priority_boost_min_value: p.priority_boost_min_value.toString() })));
      setDefaultPriority(loaded.default_priority?.toString() || '');
      setMaxPriority(loaded.max_priority?.toString() || '');
    } catch (err) {
      console.error("❌ Failed to load version", err);
    }
  };

    const applyPartitionsToAll = () => {
    if (!partitionValue || isNaN(parseInt(partitionValue))) {
      setErrorMessage('❌ Please enter a valid number of partitions.');
      return;
    }
    const updated = priorityBoost.map(p => ({
      ...p,
      number_of_partitions: partitionValue
    }));
    setPriorityBoost(updated);
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
            <button onClick={() => {
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
            }} className="button-style">Continue</button>
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

            {/* Topics Table */}
            <div className="table-header">
              <h3>Topics <button onClick={() => setTopics([...topics, { name: '', priority: '' }])} className="plus-button">➕</button></h3>
            </div>
            <table className="styled-table">
              <thead><tr><th>Topic Name</th><th>Priority</th><th>Actions</th></tr></thead>
              <tbody>
                {topics.map((topic, index) => (
                  <tr key={index}>
                    <td><input value={topic.name} onChange={(e) => {
                      const updated = [...topics]; updated[index].name = e.target.value; setTopics(updated);
                    }} className="input-style" /></td>
                    <td><input value={topic.priority} onChange={(e) => {
                      const updated = [...topics]; updated[index].priority = e.target.value; setTopics(updated);
                    }} className="input-style" /></td>
                    <td><button className="delete-button" onClick={() => setTopics(topics.filter((_, i) => i !== index))}>Delete</button></td>
                  </tr>
                ))}
              </tbody>
            </table>

            {/* Value-Based Rules */}
            <div className="table-header">
              <h3>Value-Based Rules <button onClick={() => setValRules([...valRules, { role_name: '', val: '', priority: '' }])} className="plus-button">➕</button></h3>
            </div>
            <table className="styled-table">
              <thead><tr><th>Role Name</th><th>Value</th><th>Priority</th><th>Actions</th></tr></thead>
              <tbody>
                {valRules.map((val, index) => (
                  <tr key={index}>
                    <td><input value={val.role_name} onChange={(e) => {
                      const updated = [...valRules]; updated[index].role_name = e.target.value; setValRules(updated);
                    }} className="input-style" /></td>
                    <td><input value={val.val} onChange={(e) => {
                      const updated = [...valRules]; updated[index].val = e.target.value; setValRules(updated);
                    }} className="input-style" /></td>
                    <td><input value={val.priority} onChange={(e) => {
                      const updated = [...valRules]; updated[index].priority = e.target.value; setValRules(updated);
                    }} className="input-style" /></td>
                    <td><button className="delete-button" onClick={() => setValRules(valRules.filter((_, i) => i !== index))}>Delete</button></td>
                  </tr>
                ))}
              </tbody>
            </table>

            {/* Priority Boost */}
             <div className="table-header">
            <h3>Priority Boost <button onClick={() => setPriorityBoost([...priorityBoost, { topic_name: '', priority_boost_min_value: '', number_of_partitions: '' }])} className="plus-button">➕</button></h3>
            </div>
            <div style={{ display: 'flex', gap: '10px', marginBottom: '10px' }}>
            <input
            placeholder="Set All Partitions"
            value={partitionValue}
            onChange={(e) => setPartitionValue(e.target.value)}
            className="input-style"
            />
            <button onClick={applyPartitionsToAll} className="button-style">Set All</button>
            </div>
            <table className="styled-table">
            <thead><tr><th>Topic</th><th>Boost Min Value</th><th>Partitions</th><th>Actions</th></tr></thead>
            <tbody>
            {priorityBoost.map((boost, index) => (
            <tr key={index}>
            <td><input value={boost.topic_name} onChange={(e) => {
            const updated = [...priorityBoost]; updated[index].topic_name = e.target.value; setPriorityBoost(updated);
            }} className="input-style" /></td>
            <td><input value={boost.priority_boost_min_value} onChange={(e) => {
            const updated = [...priorityBoost]; updated[index].priority_boost_min_value = e.target.value; setPriorityBoost(updated);
            }} className="input-style" /></td>
            <td><input value={boost.number_of_partitions || ''} onChange={(e) => {
            const updated = [...priorityBoost]; updated[index].number_of_partitions = e.target.value; setPriorityBoost(updated);
            }} className="input-style" /></td>
            <td><button className="delete-button" onClick={() => setPriorityBoost(priorityBoost.filter((_, i) => i !== index))}>Delete</button></td>
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
