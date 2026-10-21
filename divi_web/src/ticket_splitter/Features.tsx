import { Card, CardContent } from "./ui";
import { Calculator, Receipt, Users, Clock, Shield, Lock, MessageCircle } from "lucide-react";

const features = [
  {
    icon: MessageCircle,
    title: "Empezá sin registro",
    description: "Creá una división rápida desde el navegador, sin iniciar sesión."
  },
  {
    icon: MessageCircle,
    title: "Participantes a tu medida",
    description: "Elegí quiénes participan de cada gasto y quiénes adelantaron la plata."
  },
  {
    icon: Calculator,
    title: "Cálculo automático",
    description: "Dividí en partes iguales o asigná importes exactos, con precisión de centavos."
  },
  {
    icon: Clock,
    title: "Pagos al día",
    description: "Marcá las transferencias realizadas y deshacé un pago si te equivocaste."
  },
  {
    icon: Users,
    title: "Múltiples grupos",
    description: "Maneja gastos de diferentes grupos por separado: familia, amigos, trabajo, viajes."
  },
  {
    icon: Clock,
    title: "Historial completo",
    description: "Revisá los gastos, los pagos y el balance de cada división."
  },
  {
    icon: Shield,
    title: "Tus cuentas, a mano",
    description: "Las divisiones rápidas se guardan automáticamente en este navegador."
  },
  {
    icon: Lock,
    title: "Un espacio para cada plan",
    description: "Usá Mi espacio para abrir tus grupos con cuenta, tus finanzas y tus regalos."
  },
  {
    icon: Receipt,
    title: "Calculadora gratuita",
    description: "La división rápida es gratuita. El plan Premium de Mi espacio es opcional."
  }
];

export const Features = () => {
  return (
    <section id="features" className="py-20 bg-slate-50">
      <div className="container mx-auto px-4">
        <div className="text-center mb-16">
          <h2 className="text-4xl lg:text-5xl font-bold text-slate-900 mb-6">
            Todo lo que necesitas para
            <span className="bg-gradient-to-r from-teal-700 to-emerald-500 bg-clip-text text-transparent"> dividir gastos</span>
          </h2>
          <p className="text-xl text-slate-600 max-w-3xl mx-auto">
            Desde gastos simples hasta viajes complicados, podés organizar quién pagó, cuánto le toca a cada persona y cómo saldar la cuenta.
          </p>
        </div>

        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
          {features.map((feature, index) => (
            <Card key={index} className="bg-white shadow-lg border-0 p-6 hover:shadow-xl hover:-translate-y-1 transition-all duration-300 group">
              <CardContent className="p-0">
                <div className="flex items-center mb-4">
                  <div className="bg-gradient-to-br from-teal-700 to-emerald-500 p-3 rounded-xl mr-4 group-hover:scale-110 transition-transform duration-300">
                    <feature.icon className="h-6 w-6 text-white" />
                  </div>
                </div>
                <h3 className="text-lg font-semibold text-slate-900 mb-2">{feature.title}</h3>
                <p className="text-slate-600 text-sm leading-relaxed">{feature.description}</p>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
};
