package watch

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	zlog "github.com/rs/zerolog/log"
)

// processScriptFile reads a .ws file, compiles it, and saves/updates it as an instruction.
func processScriptFile(filePath string) {

	log := zlog.With().Str("script_file", filePath).Logger()

	// Short delay to ensure file write is complete, especially for watcher events
	time.Sleep(500 * time.Millisecond)

	scriptContentBytes, readErr := os.ReadFile(filePath)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			log.Info().Msg("Script file deleted or moved, skipping.")
			return
		}
		log.Error().Err(readErr).Msg("Failed to read script file")
		return
	}
	scriptContent := string(scriptContentBytes)

	if strings.TrimSpace(scriptContent) == "" {
		log.Info().Msg("Script file is empty, skipping processing.")
		return
	}

	ruleName, description, compiledDSLJSON, compileErr := CompileWatchScript(scriptContent)
	if compileErr != nil {
		log.Error().Err(compileErr).Msg("Failed to compile script file")
		//todo send to cloud
		return
	}

	// Try to create the instruction first
	instruction, createErr := CreateInstructionWithPrecompiledDSL(context.Background(), ruleName, scriptContent, description, compiledDSLJSON)
	if createErr != nil {
		if strings.Contains(createErr.Error(), "Constraint Error") || (strings.Contains(createErr.Error(), "Instruction with name") && strings.Contains(createErr.Error(), "already exists")) {
			existingInst, getErr := GetInstructionByName(ruleName)
			if getErr != nil {
				log.Error().Err(getErr).Str("rule_name", ruleName).Msg("Failed to retrieve existing instruction by name for update")
				return
			}

			// Now update the existing instruction
			_, updateErr := UpdateInstructionWithPrecompiledDSL(context.Background(), existingInst.ID, ruleName, scriptContent, description, compiledDSLJSON)
			if updateErr != nil {
				log.Error().Err(updateErr).Str("rule_name", ruleName).Int64("instruction_id", existingInst.ID).Msg("Failed to update existing instruction from script file")
			}
		} else {
			log.Error().Err(createErr).Str("rule_name", ruleName).Msg("Failed to create new instruction from script file (other error)")
		}
	} else {
		log.Info().Str("rule_name", instruction.Name).Int64("instruction_id", instruction.ID).Msg("Successfully created new instruction from script file")
	}
}

// processExistingScriptsInDir scans the directory for .ws files and processes them.
func processExistingScriptsInDir(dirPath string) {
	log := zlog.With().Str("directory", dirPath).Logger()

	files, err := os.ReadDir(dirPath)
	if err != nil {
		log.Error().Err(err).Msg("Failed to read script directory for initial scan")
		return
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(strings.ToLower(file.Name()), ".ws") {
			filePath := dirPath + "/" + file.Name() // Simple path join, consider filepath.Join for robustness

			processScriptFile(filePath)
		}
	}

}

// watchScriptDirectory monitors the given directory for .ws file changes.
func watchScriptDirectory(dirPath string) {
	log := zlog.With().Str("directory", dirPath).Logger()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create file watcher") // Fatal as watcher is critical if enabled
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		defer close(done)
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					log.Warn().Msg("File watcher events channel closed")
					return
				}
				log.Debug().Str("event", event.String()).Msg("Received watcher event")
				// Process .ws files on Create or Write events
				if strings.HasSuffix(strings.ToLower(event.Name), ".ws") && (event.Has(fsnotify.Create) || event.Has(fsnotify.Write)) {
					log.Info().Str("file", event.Name).Str("op", event.Op.String()).Msg("Detected relevant change to a .ws file")
					processScriptFile(event.Name) // event.Name is the full path
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					log.Warn().Msg("File watcher errors channel closed")
					return
				}
				log.Error().Err(err).Msg("File watcher error")
			}
		}
	}()

	err = watcher.Add(dirPath)
	if err != nil {
		log.Error().Err(err).Msg("Failed to add directory to watcher. Automatic script compilation will not work for this session.")
		// Don't make it fatal, the rest of the app might still be useful.
		return // Exit this goroutine if watch setup fails
	}
	log.Info().Msg("File watcher started successfully")
	<-done // Block until the event processing goroutine exits
	log.Info().Msg("File watcher stopped.")
}
