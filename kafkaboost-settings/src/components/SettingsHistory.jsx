import React from 'react';

const SettingsHistory = ({ versions, onSelectVersion }) => {
  if (!versions || versions.length === 0) {
    return <p>No previous settings found.</p>;
  }

  return (
    <div className="settings-history">
      <h3>Settings History:</h3>
      <ul style={{ listStyle: 'none', padding: 0 }}>
        {versions.map((version, idx) => {
          const fileName = version.key.split('/').pop();
          const displayName = fileName.replace('settings-', '').replace('.json', '');
          const date = version.lastModified
            ? new Date(version.lastModified).toLocaleString()
            : 'Unknown date';

          return (
            <li key={idx} style={{ marginBottom: '8px' }}>
              <button onClick={() => onSelectVersion(version.key)}>
                ⏳ {date}
              </button>
              <span style={{ marginLeft: '8px', color: '#888' }}>{fileName}</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
};

export default SettingsHistory;
