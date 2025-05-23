
import { useEffect } from 'react';
//import { Storage } from 'aws-amplify'; 
//import  {Auth}  from './node_modules/@aws-amplify';



export default function SettingsSync({ onLoad, settingsToSave }) {
  useEffect(() => {
    async function loadSettings() {
      try {
        const user = await Auth.currentAuthenticatedUser();
        const userId = user.attributes.sub;

        const result = await Storage.get(`${userId}/latest.json`, { download: true });
        const text = await result.Body.text();
        const data = JSON.parse(text);
        onLoad(data);
      } catch (err) {
        console.warn("⚠️ לא נמצאו הגדרות שמורות", err);
      }
    }

    loadSettings();
  }, [settingsToSave]);

  return null;
}
