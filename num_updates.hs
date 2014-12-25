import System.Process
import Data.List
import Control.Arrow

infixr 1 <&>
(<&>) = flip fmap

main = do
  res <- readProcess "apt-get" ["--dry-run", "upgrade"] ""
     <&> lines
     >>> filter ("Inst " `isPrefixOf`)
     >>> length
  putStrLn (show res)

