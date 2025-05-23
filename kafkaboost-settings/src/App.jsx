import React, { useState, useEffect } from 'react';
import './App.css';
import logo from './assets/kafkaboost-logo.png';
import rocketIcon from './assets/rocket-icon.png';
import SettingsSync from './components/SettingsSync';
import SettingsHistory from './components/SettingsHistory';
import { withAuthenticator } from '@aws-amplify/ui-react';
import '@aws-amplify/ui-react/styles.css';

function App() {
  const [stage, setStage] = useState('defineMax');
  const [topics, setTopics] = useState([]);
  const [valRules, setValRules] = useState([]);
  const [defaultPriority, setDefaultPriority] = useState('');
  const [maxPriority, setMaxPriority] = useState('');
  const [editingSettings, setEditingSettings] = useState(false);
  const [editingTopicIndex, setEditingTopicIndex] = useState(null);
  const [editingValRuleIndex, setEditingValRuleIndex] = useState(null);
  const [errorMessage, setErrorMessage] = useState('');
  const [successMessage, setSuccessMessage] = useState('');
  const [successFullScreen, setSuccessFullScreen] = useState(false);
  const [topicErrors, setTopicErrors] = useState({});
  const [valRuleErrors, setValRuleErrors] = useState({});

  useEffect(() => {
    console.log("App Loaded");
  }, []);

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

  const validatePriority = (priority) => {
    const num = parseInt(priority);
    return !isNaN(num) && num >= 1 && num <= parseInt(maxPriority);
  };

  const handleSubmit = () => {
    if (!defaultPriority || !maxPriority) {
      setErrorMessage('Default and Max Priority are required.');
      return;
    }

    const hasInvalidTopics = topics.some(t => t.name.trim() === '' || !validatePriority(t.priority));
    const hasInvalidValRules = valRules.some(v => v.val.trim() === '' || !validatePriority(v.priority));

    if (hasInvalidTopics || hasInvalidValRules) {
      setErrorMessage('Some settings are invalid. Please fix them before saving.');
      return;
    }

    const settings = {
      topics: topics.filter(t => t.name && t.priority),
      defaultPriority,
      priorityRange: { min: "1", max: maxPriority },
      valRules: valRules.filter(v => v.val && v.priority),
    };

    setSuccessFullScreen(true);
    setTimeout(() => {
      setSuccessFullScreen(false);
      setSuccessMessage('Settings saved successfully!');
    }, 3000);
    setErrorMessage('');
  };

  if (successFullScreen) {
    return (
      <div className="success-fullscreen">
        <h1 className="success-big-text">Settings Saved Successfully!</h1>
        <img src={rocketIcon} alt="rocket" className="rocket-animation-only" />
      </div>
    );
  }

  return (
    <div className="page-container">
      <img src={logo} alt="KafkaBoost Logo" className="logo" />
      <div className="form-container">
        <SettingsSync
          onLoad={(loadedSettings) => {
            if (loadedSettings) {
              setTopics(loadedSettings.topics || []);
              setValRules(loadedSettings.valRules || []);
              setDefaultPriority(loadedSettings.defaultPriority || '');
              setMaxPriority((loadedSettings.priorityRange || {}).max || '');
            }
          }}
          settingsToSave={{
            topics,
            defaultPriority,
            priorityRange: { min: "1", max: maxPriority },
            valRules,
          }}
        />

        <SettingsHistory
          onSelect={(selectedSettings) => {
            setTopics(selectedSettings.topics || []);
            setValRules(selectedSettings.valRules || []);
            setDefaultPriority(selectedSettings.defaultPriority || '');
            setMaxPriority((selectedSettings.priorityRange || {}).max || '');
          }}
        />

        <h2>History</h2>
        <pre>{JSON.stringify({ topics, valRules, defaultPriority, maxPriority }, null, 2)}</pre>

        <button onClick={handleSubmit}>Save All Settings</button>
      </div>
    </div>
  );
}

export default App;
