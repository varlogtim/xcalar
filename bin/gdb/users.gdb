# Quick and dirty GDB macro for dumping users, sessions, Dags etc.
# XXX Parameterize this and may be use Python instead?
def dumpAllUsers
    set $ii = 0
    print UserMgr::instance->usrsHt_
    while $ii < UserMgr::SlotsUsrsHt
        print $ii
        set $users = ('UserMgr::PerUsrInfo' *) (&UserMgr::instance->usrsHt_.slots)[0][$ii]

        while $users != 0
            set $users = ('UserMgr::PerUsrInfo' *)((uintptr_t)$users - 0x168)
            print *$users
            set $jj = 0

            while $jj < UserMgr::SlotsSessionsHt
                set $perSession = ('UserMgr::UsrPerSessInfo' *) ((&($users->sessionsHt_.slots))[0][$jj])
                while $perSession != 0
                    print $perSession
                    print *$perSession->session_
                    if $perSession->session_->sessionDag_
                        printDag $perSession->session_->sessionDag_
                    end
                    set $perSession = ('UserMgr::UsrPerSessInfo' *) $perSession->hook_.next
                end
                set $jj = $jj + 1
            end
            set $users = ('UserMgr::PerUsrInfo' *) $users->hook_.next
        end
        set $ii = $ii + 1
    end
end

document dumpAllUsers
    Dump all users, sessions of a user and DAG of a session. Note that you would
    require printDag GDB macro sourced to use this.
end
