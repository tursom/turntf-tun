package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tursom/turntf-tun/internal/tun"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "turntf-tun: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage(os.Stderr)
		return errors.New("missing command")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	configPath := fs.String("c", "config.yaml", "配置文件路径")
	fs.StringVar(configPath, "config", "config.yaml", "配置文件路径")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "run":
		cfg, err := tun.LoadConfig(*configPath)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return tun.Run(ctx, cfg, log.New(os.Stderr, "", log.LstdFlags))
	case "check-config":
		if _, err := tun.LoadConfig(*configPath); err != nil {
			return err
		}
		fmt.Println("config ok")
		return nil
	case "example-config":
		fmt.Print(tun.ExampleConfig)
		return nil
	case "help", "-h", "--help":
		usage(os.Stdout)
		return nil
	default:
		usage(os.Stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage(out *os.File) {
	fmt.Fprintln(out, "用法:")
	fmt.Fprintln(out, "  turntf-tun run -c config.yaml")
	fmt.Fprintln(out, "  turntf-tun check-config -c config.yaml")
	fmt.Fprintln(out, "  turntf-tun example-config")
}
