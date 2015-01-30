
import System.Process
import Data.List
import Control.Arrow
import System.Environment
import Control.Monad

infixr 1 <&>
(<&>) = flip fmap

wrapMany n str | n > 25    = "<fc=red>" ++ str ++ "</fc>"
               | otherwise = str

main = do
  res <- readProcess "apt-get" ["--dry-run", "upgrade"] ""
     <&> lines
     >>> filter ("Inst " `isPrefixOf`)
     >>> length
  when (res > 0) $
    putStrLn (wrapMany res ("Updates pending: " ++ show res))

