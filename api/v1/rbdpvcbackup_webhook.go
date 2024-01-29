package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var rbdpvcbackuplog = logf.Log.WithName("rbdpvcbackup-resource")

// SetupWebhookWithManager will setup the manager to manage the webhooks
func (r *RBDPVCBackup) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

//+kubebuilder:webhook:path=/mutate-backup-cybozu-com-v1-rbdpvcbackup,mutating=true,failurePolicy=fail,sideEffects=None,groups=backup.cybozu.com,resources=rbdpvcbackups,verbs=create;update,versions=v1,name=mrbdpvcbackup.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &RBDPVCBackup{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *RBDPVCBackup) Default() {
	rbdpvcbackuplog.Info("default", "name", r.Name)

	// TODO(user): fill in your defaulting logic.
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
//+kubebuilder:webhook:path=/validate-backup-cybozu-com-v1-rbdpvcbackup,mutating=false,failurePolicy=fail,sideEffects=None,groups=backup.cybozu.com,resources=rbdpvcbackups,verbs=create;update,versions=v1,name=vrbdpvcbackup.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &RBDPVCBackup{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *RBDPVCBackup) ValidateCreate() (admission.Warnings, error) {
	rbdpvcbackuplog.Info("validate create", "name", r.Name)

	// TODO(user): fill in your validation logic upon object creation.
	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *RBDPVCBackup) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	rbdpvcbackuplog.Info("validate update", "name", r.Name)

	// TODO(user): fill in your validation logic upon object update.
	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *RBDPVCBackup) ValidateDelete() (admission.Warnings, error) {
	rbdpvcbackuplog.Info("validate delete", "name", r.Name)

	// TODO(user): fill in your validation logic upon object deletion.
	return nil, nil
}
