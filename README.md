# flipr(8)

                                       *"  %_
                             _........#     %_
                       _^  '              "%@"
                      .^                      ""*_
                    -@-*+++**-.._____             ^*_
                                  %@*_""^**.__       ^*_
                                    ^*@        ^*._     "*_
                                                   '^*._  '%_
                                                        ^*._ ^._       _._
                          _#"\                              ^*.^%___*^" _#
                __..******" __%                                '%.'    #
               _#            '^*._                                _    *_
              /+^^^=+**_@.___     "*_                               ""*_ %_
                        "%_  '""*__  %._                                %_@
                                   "%*_'*_
                                       ^%."._.*^\#
                                           ^_  #
                                            ^"*.^.
                                                ^+"

## name

flipr -- the flag store: ask before you do anything expensive

## synopsis

    flipr [-addr host:port] [-db path] [-oplog path]
          [-kafka brokers] [-topic name]

## description

a flag is a bool, a string or an integer; a namespace is one service at
one version. a deploy declares flags and never sets values; an operator
flips them with a reason, which is recorded.

if flipr is down the network is down, on purpose. protojson over plain
http, one route per proto method, the contract at `/api`, and clients
for go, python and typescript in `clients/`.

## see also

USING.md, DESIGN.md, OPERATION.md, RUNBOOK-restore.md, LICENSE.txt, and
`art/flipr-dist.ansi`, which game paints when a build finishes.
