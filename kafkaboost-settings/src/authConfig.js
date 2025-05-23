const authConfig = {
  Auth: {
    region: 'us-east-1',
    userPoolId: 'us-east-1_UWnIqsEAY',
    userPoolWebClientId: 'kjjdnu5tlesmonjq39aeik5ms',
    oauth: {
      domain: 'us-east-1uwniqseay.auth.us-east-1.amazoncognito.com',
      scope: ['email', 'openid', 'profile'],
      redirectSignIn: 'https://master.d158m42rfyjjwz.amplifyapp.com/',
      redirectSignOut: 'https://master.d158m42rfyjjwz.amplifyapp.com/',
      responseType: 'code',
    },
  },
  Storage: {
    AWSS3: {
      bucket: 'kafkaboost-user-settings',
      region: 'us-east-1',
    },
  },
};

export default authConfig;
