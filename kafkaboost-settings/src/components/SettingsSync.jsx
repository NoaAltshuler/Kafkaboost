import React from 'react';

function SettingsSync({ versions, onSelectVersion }) {
  return (
    <div style={{ marginBottom: '30px' }}>
      <h3>Settings History:</h3>
      {versions.length === 0 ? (
        <p>No previous settings found.</p>
      ) : (
        <ul>
          {versions.map((version) => {
            const timestamp = new Date(version.lastModified).toLocaleString('he-IL');
            return (
              <li key={version.key}>
                <button onClick={() => onSelectVersion(version.key)} style={{ background: 'none', border: 'none', color: 'blue', cursor: 'pointer' }}>
                  📅 {timestamp}
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

export default SettingsSync;
