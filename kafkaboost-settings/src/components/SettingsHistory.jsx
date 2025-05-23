import { useEffect, useState } from 'react';
import { Storage } from 'aws-amplify';
import { getCurrentUser } from 'aws-amplify/auth';

export default function SettingsHistory({ onSelect }) {
  const [versions, setVersions] = useState([]);

  useEffect(() => {
    async function fetchVersions() {
      try {
        const user = await getCurrentUser();
        const userId = user.userId;

        const result = await Storage.list(`${userId}/`);
        const files = result
          .filter(item =>
            item.key.endsWith('.json') &&
            !item.key.includes('latest')
          )
          .sort((a, b) =>
            new Date(b.lastModified || b.key) - new Date(a.lastModified || a.key)
          );

        setVersions(files);
      } catch (err) {
        console.error("⚠️ לא ניתן לטעון גרסאות מה-S3:", err);
      }
    }

    fetchVersions();
  }, []);

  const handleSelect = async (e) => {
    const key = e.target.value;
    if (!key) return;

    try {
      const file = await Storage.get(key, { download: true });
      const text = await file.Body.text();
      const json = JSON.parse(text);
      onSelect(json);
    } catch (err) {
      console.error("❌ שגיאה בטעינת גרסה:", err);
    }
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
