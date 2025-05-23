import { useEffect } from 'react';
import { Storage } from 'aws-amplify';
import { getCurrentUser } from 'aws-amplify/auth';

export default function SettingsSync({ onLoad, settingsToSave }) {
  useEffect(() => {
    async function loadSettings() {
      try {
        const user = await getCurrentUser();
        const userId = user.userId;

        const result = await Storage.get(`${userId}/latest.json`, { download: true });
        const text = await result.Body.text();
        const data = JSON.parse(text);
        onLoad(data);
      } catch (err) {
        console.warn("⚠️ לא נמצאו הגדרות שמורות", err);
      }
    }

    loadSettings();
  }, [onLoad]);

  useEffect(() => {
    async function saveSettings() {
      if (!settingsToSave) return;

      const user = await getCurrentUser();
      const userId = user.userId;
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const content = JSON.stringify(settingsToSave);

      await Storage.put(`${userId}/${timestamp}.json`, content, {
        contentType: 'application/json',
      });

      await Storage.put(`${userId}/latest.json`, content, {
        contentType: 'application/json',
      });

      console.log("✅ הגדרות נשמרו ב-S3");
    }

    saveSettings();
  }, [settingsToSave]);

  return null;
}
