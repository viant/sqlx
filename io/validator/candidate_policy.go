package validator

import (
	"fmt"
	"reflect"

	"github.com/viant/sqlx/io"
)

func (o *Options) validateCandidatePolicyConflicts() error {
	if !o.candidatePoliciesSet {
		return nil
	}
	if !o.Shallow {
		return fmt.Errorf("candidate policy validation requires shallow rows; validate nested matched rows separately")
	}
	if o.previousSet {
		return fmt.Errorf("WithCandidatePolicies conflicts with WithPrevious")
	}
	if o.fieldFilterSet {
		return fmt.Errorf("WithCandidatePolicies conflicts with WithFieldFilter")
	}
	if o.SetMarker != nil {
		return fmt.Errorf("WithCandidatePolicies conflicts with WithSetMarker")
	}
	return nil
}

func (o *Options) validateCandidatePolicies(input any, at io.ValueAccessor, count int) error {
	if !o.candidatePoliciesSet {
		return nil
	}
	if len(o.CandidatePolicies) != count {
		return fmt.Errorf("candidate policy count %d does not match candidate count %d", len(o.CandidatePolicies), count)
	}
	var candidateType reflect.Type
	declaredType := reflect.TypeOf(input)
	if declaredType.Kind() == reflect.Ptr {
		declaredType = declaredType.Elem()
	}
	if declaredType.Kind() == reflect.Slice {
		declaredType = declaredType.Elem()
		// Interface batches are checked by their concrete entries; empty ones
		// have no row type to validate. Typed batches retain their type when empty.
		if declaredType.Kind() != reflect.Interface {
			if declaredType.Kind() == reflect.Ptr {
				declaredType = declaredType.Elem()
			}
			if declaredType.Kind() != reflect.Struct {
				return fmt.Errorf("candidate element type %v is not a struct", declaredType)
			}
			candidateType = declaredType
		}
	}
	// Normalization belongs to this invocation, never to the caller's slice.
	o.CandidatePolicies = append([]CandidatePolicy(nil), o.CandidatePolicies...)
	for i := 0; i < count; i++ {
		current := reflect.ValueOf(at(i))
		if !current.IsValid() || (current.Kind() == reflect.Ptr && current.IsNil()) {
			return fmt.Errorf("candidate %d is nil", i)
		}
		currentType := current.Type()
		if currentType.Kind() == reflect.Ptr {
			currentType = currentType.Elem()
		}
		if currentType.Kind() != reflect.Struct {
			return fmt.Errorf("candidate %d type %v is not a struct", i, currentType)
		}
		if candidateType == nil {
			candidateType = currentType
		} else if currentType != candidateType {
			return fmt.Errorf("candidate %d type %v does not match %v", i, currentType, candidateType)
		}
		if err := o.CandidatePolicies[i].normalizePrevious(candidateType, i); err != nil {
			return err
		}
		if err := o.CandidatePolicies[i].validateProducerPolicy(); err != nil {
			return fmt.Errorf("candidate %d: %w", i, err)
		}
	}
	return nil
}

func (p *CandidatePolicy) validateProducerPolicy() error {
	if p.DeferredFields != nil && p.Previous != nil {
		return fmt.Errorf("DeferredFields requires nil Previous")
	}
	if len(p.SatisfiedReferences) != 0 && (p.Previous != nil || p.DeferredFields != nil) {
		return fmt.Errorf("SatisfiedReferences requires nil Previous and nil DeferredFields")
	}
	return nil
}

func (o *Options) validateCandidateReferences(checks *Checks) error {
	if !o.candidatePoliciesSet {
		return nil
	}
	for i := range o.CandidatePolicies {
		if err := checks.ValidateReferences(o.CandidatePolicies[i].SatisfiedReferences); err != nil {
			return fmt.Errorf("candidate %d: %w", i, err)
		}
	}
	return nil
}

func (p *CandidatePolicy) normalizePrevious(candidateType reflect.Type, index int) error {
	if p.Previous == nil {
		return nil
	}
	previous := reflect.ValueOf(p.Previous)
	previousType := previous.Type()
	if previousType.Kind() == reflect.Ptr {
		previousType = previousType.Elem()
	}
	if previousType != candidateType {
		return fmt.Errorf("previous row %d type %v does not match %v", index, previousType, candidateType)
	}
	if previous.Kind() == reflect.Ptr && previous.IsNil() {
		p.Previous = nil
	}
	return nil
}
