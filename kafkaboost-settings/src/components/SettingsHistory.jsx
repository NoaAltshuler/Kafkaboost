import { useEffect, useState } from 'react';
import { Storage, Auth } from 'aws-amplify';

export default function SettingsHistory({ onSelect }) {
  const [versions, setVersions] = useState([]);

  useEffect(() => {
    async function fetchVersions() {
      const user = await Auth.currentAuthenticatedUser();
      const userId = user.attributes.sub;

      const result = await Storage.list(`${userId}/`);
      const files = result
        .filter(item => item.key.endsWith('.json') && !item.key.includes('latest'))
        .sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified)); // הכי חדשה למעלה

      setVersions(files);
    }

    fetchVersions();
  }, []);

  const handleSelect = async (e) => {
    const key = e.target.value;
    if (!key) return;

    const file = await Storage.get(key, { download: true });
    const text = await file.Body.text();
    const json = JSON.parse(text);

    onSelect(json);
  };

  return (
    <div>
      <h3>בחר גרסה קודמת</h3>
      <select onChange={handleSelect}>
        <option value="">-- בחר גרסה --</option>
        {versions.map((v, idx) => (
          <option key={idx} value={v.key}>
            {v.key.split('/')[1].replace('.json', '')}
          </option>
        ))}
      </select>
    </div>
  );
}
