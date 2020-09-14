-- Interpret using this command:
--    podman run -it --rm -v $(pwd):/data:Z quay.io/software-factory/shake-factory
--
-- Learn more at: https://softwarefactory-project.io/cgit/software-factory/shake-factory/tree/README.md

import Development.Shake
import ShakeFactory
import ShakeFactory.Dhall
import ShakeFactory.Python

main :: IO ()
main = shakeMain $ do
  want [".zuul.yaml"]
  ".zuul.yaml" %> dhallZuulAction "(./.sf.dhall).zuul"
  phony "docs" (pythonDocAction "purgelogs")
