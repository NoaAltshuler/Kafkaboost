const authConfig = {
  Auth: {
    region: 'us-east-1',
    userPoolId: 'us-east-1_BWuzmkkZC', // כמו שמופיע ב־User Pool Overview
  // userPoolWebClientId: '37lctucgsu8p2bkg5m6rb2h4lo', // כמו שמופיע ב־App Client
    oauth: {
      domain: 'us-east-1bwuzmkkzc.auth.us-east-1.amazoncognito.com', // מה־Domain שהגדרת
      scope: ['email', 'openid', 'profile'],
      redirectSignIn: 'https://master.d158m42rfyjjwz.amplifyapp.com/',
      redirectSignOut: 'https://master.d158m42rfyjjwz.amplifyapp.com/',
      responseType: 'code',
    },
  },
  Storage: {
    AWSS3: {
      bucket: 'kafkaboost-user-settings', // השם המדויק של ה־S3 bucket
      region: 'us-east-1',
    },
  },
};

export default authConfig;
