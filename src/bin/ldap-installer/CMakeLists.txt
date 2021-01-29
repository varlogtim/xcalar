install(FILES
            ldapBaseInstaller.py ldapEl6Installer.py ldapEl7Installer.py
            ldapElInstaller.py ldapInstaller.py ldapLdif.py sslCaInstaller.py
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                    GROUP_READ GROUP_WRITE GROUP_EXECUTE
                    WORLD_READ             WORLD_EXECUTE
        DESTINATION scripts)
