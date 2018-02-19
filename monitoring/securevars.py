import boto


def get_securevar_by_key(securevar_key):
    """
    TODO(jones) this is taken from SMS service. We should set up a way to share code between
    Python3 libraries (an internal pypi perhaps) just like we have for Go with `gott`

    Given a key value, this function will return the text contained within the corresponding
    securevar file in S3. For example, passing in a key of "twilio_staging_account_sid" will
    return the string contents of the file "securevar_twilio_staging_account_sid" in the s3
    bucket tt-puppet-securevars.
    Securevars are managed this way so that every machine managed by puppet will also share
    these security credentials as environment variables.

    Note: If you're going to use a new securevar, make sure to set up the permissions on Amazon's
    web console (see below). Also add your new permission to donatello's tophat.py file.
    1) Go to console.aws.amazon.com
    2) Click on Identity & Access Management
    3) Search for the tophat-webserver-securevars-policy files and update them.
    """
    securevar_key = 'securevar_{}'.format(securevar_key)
    connection = boto.connect_s3()
    bucket = connection.get_bucket('tt-puppet-securevars')
    key = bucket.get_key(securevar_key)
    if key:
        # Remove trailing newlines from s3 bucket values
        return key.get_contents_as_string().decode('utf-8').strip()
    else:
        raise KeyError('The key {} does not exist in the tt-puppet-securevars s3 bucket.'.format(
            securevar_key
        ))