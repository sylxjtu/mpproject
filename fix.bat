mvn assembly:assembly -DdescriptorId=jar-with-dependencies
git add .
git commit -m "fix"
git push origin trie