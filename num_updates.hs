{-# LANGUAGE OverloadedStrings #-}

import           Turtle
import qualified Control.Foldl as L (length)
import           Control.Monad      (ap)

main = wcL (candidates dryUpgrade) >>= ap notify pending

wcL = flip fold L.length
candidates = grep (prefix "Inst")
dryUpgrade = inshell "apt-get --dry-run upgrade" ""

notify n | n > 25    = alert
         | n > 0     = echo
         | otherwise = const (return ())

pending = format ("Updates pending: "%d)

alert = echo . format ("<fc=red>"%s%"</fc>")
