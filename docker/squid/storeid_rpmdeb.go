package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	// re := regexp.MustCompile(`[^/]+\.(rpm|deb)`)
	allre := []*regexp.Regexp{
		// regexp.MustCompile(`[^/]+\.(rpm|deb)`),
		regexp.MustCompile(`[^/]+\.(rpm|deb|jar|pom|sha1|tar\.gz)$`),
		regexp.MustCompile(`repodata/[0-9a-f]{40,64}-(filelists|primary|other|prestodelta)\.(xml|sqlite)\.(xz|gz|bz2)`),
	}

	for true {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Got line: %s\nerror: %v\n", line, err)
			}
			break
		}
		parts := strings.Split(line, " ")
		// url := strings.TrimSpace(parts[0])
		url := strings.TrimSuffix(strings.TrimSpace(parts[0]), "?")
		result := url
		for _, re := range allre {
			filename := re.FindString(url)
			if len(filename) > 0 {
				result = filename
				break
			}
		}
		fmt.Printf("OK store-id=%v\n", result)
	}
	fmt.Fprintf(os.Stderr, "Exiting from storeid helper\n")
}
