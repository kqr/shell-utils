{-# LANGUAGE OverloadedStrings #-}
import Turtle
import System.Environment
import Data.Monoid
import System.FilePath as FP

main = fmap (map read) getArgs >>= \(limit:_) -> print (FP.concat (replicate limit ".."))
